pub mod wallet;

use ff::PrimeField;
use group::GroupEncoding;
use rand_core::{OsRng, RngCore};
use std::collections::HashMap;
use std::fmt::Debug;
use std::{cmp, fmt};
use zcash_client_backend::data_api::wallet::ANCHOR_OFFSET;
use zcash_client_backend::data_api::PrunedBlock;
use zcash_client_backend::data_api::ReceivedTransaction;
use zcash_client_backend::data_api::SentTransaction;
use zcash_client_backend::proto::compact_formats::{
    CompactBlock, CompactOutput, CompactSpend, CompactTx,
};
use zcash_client_backend::wallet::SpendableNote;
use zcash_client_backend::wallet::{AccountId, WalletShieldedOutput};
use zcash_client_backend::DecryptedOutput;
use zcash_primitives::block::BlockHash;
use zcash_primitives::consensus::BlockHeight;
use zcash_primitives::memo::{Memo, MemoBytes};
use zcash_primitives::merkle_tree::CommitmentTree;
use zcash_primitives::merkle_tree::IncrementalWitness;
use zcash_primitives::sapling::{Node, Note, Nullifier, PaymentAddress};
use zcash_primitives::transaction::components::Amount;
use zcash_primitives::transaction::TxId;
use zcash_primitives::zip32::ExtendedFullViewingKey;
use zcash_primitives::{
    consensus::Network,
    sapling::{note_encryption::sapling_note_encryption, util::generate_random_rseed},
};

#[async_trait::async_trait]
pub trait WalletRead: Send + Sync + 'static {
    type Error;
    type NoteRef: Debug;
    type TxRef: Copy + Debug;

    /// Returns the minimum and maximum block heights for stored blocks.
    ///
    /// This will return `Ok(None)` if no block data is present in the database.
    async fn block_height_extrema(&self)
        -> Result<Option<(BlockHeight, BlockHeight)>, Self::Error>;

    /// Returns the default target height (for the block in which a new
    /// transaction would be mined) and anchor height (to use for a new
    /// transaction), given the range of block heights that the backend
    /// knows about.
    ///
    /// This will return `Ok(None)` if no block data is present in the database.
    async fn get_target_and_anchor_heights(
        &self,
    ) -> Result<Option<(BlockHeight, BlockHeight)>, Self::Error> {
        self.block_height_extrema().await.map(|heights| {
            heights.map(|(min_height, max_height)| {
                let target_height = max_height + 1;

                // Select an anchor ANCHOR_OFFSET back from the target block,
                // unless that would be before the earliest block we have.
                let anchor_height = BlockHeight::from(cmp::max(
                    u32::from(target_height).saturating_sub(ANCHOR_OFFSET),
                    u32::from(min_height),
                ));

                (target_height, anchor_height)
            })
        })
    }

    /// Returns the block hash for the block at the given height, if the
    /// associated block data is available. Returns `Ok(None)` if the hash
    /// is not found in the database.
    async fn get_block_hash(
        &self,
        block_height: BlockHeight,
    ) -> Result<Option<BlockHash>, Self::Error>;

    /// Returns the block hash for the block at the maximum height known
    /// in stored data.
    ///
    /// This will return `Ok(None)` if no block data is present in the database.
    async fn get_max_height_hash(&self) -> Result<Option<(BlockHeight, BlockHash)>, Self::Error> {
        let extrema = self.block_height_extrema().await?;
        let res = if let Some((_, max_height)) = extrema {
            self.get_block_hash(max_height)
                .await
                .map(|hash_opt| hash_opt.map(move |hash| (max_height, hash)))?
        } else {
            None
        };

        Ok(res)
    }

    /// Returns the block height in which the specified transaction was mined,
    /// or `Ok(None)` if the transaction is not mined in the main chain.
    async fn get_tx_height(&self, txid: TxId) -> Result<Option<BlockHeight>, Self::Error>;

    /// Returns the payment address for the specified account, if the account
    /// identifier specified refers to a valid account for this wallet.
    ///
    /// This will return `Ok(None)` if the account identifier does not correspond
    /// to a known account.
    async fn get_address(&self, account: AccountId) -> Result<Option<PaymentAddress>, Self::Error>;

    /// Returns all extended full viewing keys known about by this wallet.
    async fn get_extended_full_viewing_keys(
        &self,
    ) -> Result<HashMap<AccountId, ExtendedFullViewingKey>, Self::Error>;

    /// Checks whether the specified extended full viewing key is
    /// associated with the account.
    async fn is_valid_account_extfvk(
        &self,
        account: AccountId,
        extfvk: &ExtendedFullViewingKey,
    ) -> Result<bool, Self::Error>;

    /// Returns the wallet balance for an account as of the specified block
    /// height.
    ///
    /// This may be used to obtain a balance that ignores notes that have been
    /// received so recently that they are not yet deemed spendable.
    async fn get_balance_at(
        &self,
        account: AccountId,
        anchor_height: BlockHeight,
    ) -> Result<Amount, Self::Error>;

    /// Returns the memo for a note.
    ///
    /// Implementations of this method must return an error if the note identifier
    /// does not appear in the backing data store.
    async fn get_memo(&self, id_note: Self::NoteRef) -> Result<Memo, Self::Error>;

    /// Returns the note commitment tree at the specified block height.
    async fn get_commitment_tree(
        &self,
        block_height: BlockHeight,
    ) -> Result<Option<CommitmentTree<Node>>, Self::Error>;

    /// Returns the incremental witnesses as of the specified block height.
    #[allow(clippy::type_complexity)]
    async fn get_witnesses(
        &self,
        block_height: BlockHeight,
    ) -> Result<Vec<(Self::NoteRef, IncrementalWitness<Node>)>, Self::Error>;

    /// Returns the unspent nullifiers, along with the account identifiers
    /// with which they are associated.
    async fn get_nullifiers(&self) -> Result<Vec<(AccountId, Nullifier)>, Self::Error>;

    /// Return all spendable notes.
    async fn get_spendable_notes(
        &self,
        account: AccountId,
        anchor_height: BlockHeight,
    ) -> Result<Vec<SpendableNote>, Self::Error>;

    /// Returns a list of spendable notes sufficient to cover the specified
    /// target value, if possible.
    async fn select_spendable_notes(
        &self,
        account: AccountId,
        target_value: Amount,
        anchor_height: BlockHeight,
    ) -> Result<Vec<SpendableNote>, Self::Error>;
}

/// This trait encapsulates the write capabilities required to update stored
/// wallet data.
#[async_trait::async_trait]
pub trait WalletWrite: WalletRead {
    #[allow(clippy::type_complexity)]
    async fn advance_by_block(
        &mut self,
        block: &PrunedBlock,
        updated_witnesses: &[(Self::NoteRef, IncrementalWitness<Node>)],
    ) -> Result<Vec<(Self::NoteRef, IncrementalWitness<Node>)>, Self::Error>;

    async fn store_received_tx(
        &mut self,
        received_tx: &ReceivedTransaction,
    ) -> Result<Self::TxRef, Self::Error>;

    async fn store_sent_tx(
        &mut self,
        sent_tx: &SentTransaction,
    ) -> Result<Self::TxRef, Self::Error>;

    /// Rewinds the wallet database to the specified height.
    ///
    /// This method assumes that the state of the underlying data store is
    /// consistent up to a particular block height. Since it is possible that
    /// a chain reorg might invalidate some stored state, this method must be
    /// implemented in order to allow users of this API to "reset" the data store
    /// to correctly represent chainstate as of a specified block height.
    ///
    /// After calling this method, the block at the given height will be the
    /// most recent block and all other operations will treat this block
    /// as the chain tip for balance determination purposes.
    ///
    /// There may be restrictions on how far it is possible to rewind.
    async fn rewind_to_height(&mut self, block_height: BlockHeight) -> Result<(), Self::Error>;
}

/// This trait provides a generalization over shielded output representations.
pub trait ShieldedOutput {
    fn index(&self) -> usize;
    fn account(&self) -> AccountId;
    fn to(&self) -> &PaymentAddress;
    fn note(&self) -> &Note;
    fn memo(&self) -> Option<&MemoBytes>;
    fn is_change(&self) -> Option<bool>;
    fn nullifier(&self) -> Option<Nullifier>;
}

impl ShieldedOutput for WalletShieldedOutput<Nullifier> {
    fn index(&self) -> usize {
        self.index
    }
    fn account(&self) -> AccountId {
        self.account
    }
    fn to(&self) -> &PaymentAddress {
        &self.to
    }
    fn note(&self) -> &Note {
        &self.note
    }
    fn memo(&self) -> Option<&MemoBytes> {
        None
    }
    fn is_change(&self) -> Option<bool> {
        Some(self.is_change)
    }

    fn nullifier(&self) -> Option<Nullifier> {
        Some(self.nf)
    }
}

impl ShieldedOutput for DecryptedOutput {
    fn index(&self) -> usize {
        self.index
    }
    fn account(&self) -> AccountId {
        self.account
    }
    fn to(&self) -> &PaymentAddress {
        &self.to
    }
    fn note(&self) -> &Note {
        &self.note
    }
    fn memo(&self) -> Option<&MemoBytes> {
        Some(&self.memo)
    }
    fn is_change(&self) -> Option<bool> {
        None
    }
    fn nullifier(&self) -> Option<Nullifier> {
        None
    }
}

/// A newtype wrapper for sqlite primary key values for the notes
/// table.
#[derive(Debug, Copy, Clone)]
pub enum NoteId {
    SentNoteId(i64),
    ReceivedNoteId(i64),
}

impl fmt::Display for NoteId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NoteId::SentNoteId(id) => write!(f, "Sent Note {}", id),
            NoteId::ReceivedNoteId(id) => write!(f, "Received Note {}", id),
        }
    }
}

pub(crate) fn test_network() -> Network {
    Network::TestNetwork
}

/// Create a fake CompactBlock at the given height, containing a single output paying
/// the given address. Returns the CompactBlock and the nullifier for the new note.
pub fn fake_compact_block(
    height: BlockHeight,
    prev_hash: BlockHash,
    extfvk: ExtendedFullViewingKey,
    value: Amount,
) -> (CompactBlock, Nullifier) {
    let to = extfvk.default_address().unwrap().1;

    // Create a fake Note for the account
    let mut rng = OsRng;
    let rseed = generate_random_rseed(&test_network(), height, &mut rng);
    let note = Note {
        g_d: to.diversifier().g_d().unwrap(),
        pk_d: *to.pk_d(),
        value: value.into(),
        rseed,
    };
    let encryptor = sapling_note_encryption::<_, Network>(
        Some(extfvk.fvk.ovk),
        note.clone(),
        to,
        MemoBytes::empty(),
        &mut rng,
    );
    let cmu = note.cmu().to_repr().as_ref().to_vec();
    let epk = encryptor.epk().to_bytes().to_vec();
    let enc_ciphertext = encryptor.encrypt_note_plaintext();

    // Create a fake CompactBlock containing the note
    let mut cout = CompactOutput::new();
    cout.set_cmu(cmu);
    cout.set_epk(epk);
    cout.set_ciphertext(enc_ciphertext.as_ref()[..52].to_vec());
    let mut ctx = CompactTx::new();
    let mut txid = vec![0; 32];
    rng.fill_bytes(&mut txid);
    ctx.set_hash(txid);
    ctx.outputs.push(cout);
    let mut cb = CompactBlock::new();
    cb.set_height(u64::from(height));
    cb.hash.resize(32, 0);
    rng.fill_bytes(&mut cb.hash);
    cb.prevHash.extend_from_slice(&prev_hash.0);
    cb.vtx.push(ctx);
    (cb, note.nf(&extfvk.fvk.vk, 0))
}

/// Create a fake CompactBlock at the given height, spending a single note from the
/// given address.
pub fn fake_compact_block_spending(
    height: BlockHeight,
    prev_hash: BlockHash,
    (nf, in_value): (Nullifier, Amount),
    extfvk: ExtendedFullViewingKey,
    to: PaymentAddress,
    value: Amount,
) -> CompactBlock {
    let mut rng = OsRng;
    let rseed = generate_random_rseed(&test_network(), height, &mut rng);

    // Create a fake CompactBlock containing the note
    let mut cspend = CompactSpend::new();
    cspend.set_nf(nf.to_vec());
    let mut ctx = CompactTx::new();
    let mut txid = vec![0; 32];
    rng.fill_bytes(&mut txid);
    ctx.set_hash(txid);
    ctx.spends.push(cspend);

    // Create a fake Note for the payment
    ctx.outputs.push({
        let note = Note {
            g_d: to.diversifier().g_d().unwrap(),
            pk_d: *to.pk_d(),
            value: value.into(),
            rseed,
        };
        let encryptor = sapling_note_encryption::<_, Network>(
            Some(extfvk.fvk.ovk),
            note.clone(),
            to,
            MemoBytes::empty(),
            &mut rng,
        );
        let cmu = note.cmu().to_repr().as_ref().to_vec();
        let epk = encryptor.epk().to_bytes().to_vec();
        let enc_ciphertext = encryptor.encrypt_note_plaintext();

        let mut cout = CompactOutput::new();
        cout.set_cmu(cmu);
        cout.set_epk(epk);
        cout.set_ciphertext(enc_ciphertext.as_ref()[..52].to_vec());
        cout
    });

    // Create a fake Note for the change
    ctx.outputs.push({
        let change_addr = extfvk.default_address().unwrap().1;
        let rseed = generate_random_rseed(&test_network(), height, &mut rng);
        let note = Note {
            g_d: change_addr.diversifier().g_d().unwrap(),
            pk_d: *change_addr.pk_d(),
            value: (in_value - value).into(),
            rseed,
        };
        let encryptor = sapling_note_encryption::<_, Network>(
            Some(extfvk.fvk.ovk),
            note.clone(),
            change_addr,
            MemoBytes::empty(),
            &mut rng,
        );
        let cmu = note.cmu().to_repr().as_ref().to_vec();
        let epk = encryptor.epk().to_bytes().to_vec();
        let enc_ciphertext = encryptor.encrypt_note_plaintext();

        let mut cout = CompactOutput::new();
        cout.set_cmu(cmu);
        cout.set_epk(epk);
        cout.set_ciphertext(enc_ciphertext.as_ref()[..52].to_vec());
        cout
    });

    let mut cb = CompactBlock::new();
    cb.set_height(u64::from(height));
    cb.hash.resize(32, 0);
    rng.fill_bytes(&mut cb.hash);
    cb.prevHash.extend_from_slice(&prev_hash.0);
    cb.vtx.push(ctx);
    cb
}
