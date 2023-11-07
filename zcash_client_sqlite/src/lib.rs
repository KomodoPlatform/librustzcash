//! *An SQLite-based Zcash light client.*
//!
//! `zcash_client_sqlite` contains complete SQLite-based implementations of the [`WalletRead`],
//! [`WalletWrite`], and [`BlockSource`] traits from the [`zcash_client_backend`] crate. In
//! combination with [`zcash_client_backend`], it provides a full implementation of a SQLite-backed
//! client for the Zcash network.
//!
//! # Design
//!
//! The light client is built around two SQLite databases:
//!
//! - A cache database, used to inform the light client about new [`CompactBlock`]s. It is
//!   read-only within all light client APIs *except* for [`init_cache_database`] which
//!   can be used to initialize the database.
//!
//! - A data database, where the light client's state is stored. It is read-write within
//!   the light client APIs, and **assumed to be read-only outside these APIs**. Callers
//!   **MUST NOT** write to the database without using these APIs. Callers **MAY** read
//!   the database directly in order to extract information for display to users.
//!
//! # Features
//!
//! The `mainnet` feature configures the light client for use with the Zcash mainnet. By
//! default, the light client is configured for use with the Zcash testnet.
//!
//! [`WalletRead`]: zcash_client_backend::data_api::WalletRead
//! [`WalletWrite`]: zcash_client_backend::data_api::WalletWrite
//! [`BlockSource`]: zcash_client_backend::data_api::BlockSource
//! [`CompactBlock`]: zcash_client_backend::proto::compact_formats::CompactBlock
//! [`init_cache_database`]: crate::chain::init::init_cache_database

// Catch documentation errors caused by code changes.
#![deny(broken_intra_doc_links)]

extern crate core;

use std::collections::HashMap;
use std::path::Path;

use rusqlite::{Connection, Statement};

use zcash_primitives::{
    block::BlockHash,
    consensus::{self, BlockHeight},
    memo::Memo,
    merkle_tree::{CommitmentTree, IncrementalWitness},
    sapling::{Node, Nullifier, PaymentAddress},
    transaction::{components::Amount, TxId},
    zip32::ExtendedFullViewingKey,
};

use zcash_client_backend::{
    data_api::{
        BlockSource, PrunedBlock, ReceivedTransaction, SentTransaction, WalletRead, WalletWrite,
    },
    encoding::encode_payment_address,
    proto::compact_formats::CompactBlock,
    wallet::{AccountId, SpendableNote},
};
use zcash_extras::NoteId;

use crate::error::SqliteClientError;

pub mod chain;
pub mod error;
pub mod for_async;
pub mod wallet;

/// A wrapper for the SQLite connection to the wallet database.
pub struct WalletDb<P> {
    conn: Connection,
    params: P,
}

impl<P: consensus::Parameters> WalletDb<P> {
    pub fn sql_conn(&self) -> &Connection {
        &self.conn
    }

    /// Construct a connection to the wallet database stored at the specified path.
    pub fn for_path<F: AsRef<Path>>(path: F, params: P) -> Result<Self, rusqlite::Error> {
        Connection::open(path).map(move |conn| WalletDb { conn, params })
    }

    /// Given a wallet database connection, obtain a handle for the write operations
    /// for that database. This operation may eagerly initialize and cache sqlite
    /// prepared statements that are used in write operations.
    pub fn get_update_ops(&self) -> Result<DataConnStmtCache<'_, P>, SqliteClientError> {
        Ok(
            DataConnStmtCache {
                wallet_db: self,
                stmt_insert_block: self.conn.prepare(
                    "INSERT INTO blocks (height, hash, time, sapling_tree)
                    VALUES (?, ?, ?, ?)",
                )?,
                stmt_insert_tx_meta: self.conn.prepare(
                    "INSERT INTO transactions (txid, block, tx_index)
                    VALUES (?, ?, ?)",
                )?,
                stmt_update_tx_meta: self.conn.prepare(
                    "UPDATE transactions
                    SET block = ?, tx_index = ? WHERE txid = ?",
                )?,
                stmt_insert_tx_data: self.conn.prepare(
                    "INSERT INTO transactions (txid, created, expiry_height, raw)
                    VALUES (?, ?, ?, ?)",
                )?,
                stmt_update_tx_data: self.conn.prepare(
                    "UPDATE transactions
                    SET expiry_height = ?, raw = ? WHERE txid = ?",
                )?,
                stmt_select_tx_ref: self.conn.prepare(
                    "SELECT id_tx FROM transactions WHERE txid = ?",
                )?,
                stmt_mark_recived_note_spent: self.conn.prepare(
                    "UPDATE received_notes SET spent = ? WHERE nf = ?"
                )?,
                stmt_insert_received_note: self.conn.prepare(
                    "INSERT INTO received_notes (tx, output_index, account, diversifier, value, rcm, memo, nf, is_change)
                    VALUES (:tx, :output_index, :account, :diversifier, :value, :rcm, :memo, :nf, :is_change)",
                )?,
                stmt_update_received_note: self.conn.prepare(
                    "UPDATE received_notes
                    SET account = :account,
                        diversifier = :diversifier,
                        value = :value,
                        rcm = :rcm,
                        nf = IFNULL(:nf, nf),
                        memo = IFNULL(:memo, memo),
                        is_change = IFNULL(:is_change, is_change)
                    WHERE tx = :tx AND output_index = :output_index",
                )?,
                stmt_select_received_note: self.conn.prepare(
                    "SELECT id_note FROM received_notes WHERE tx = ? AND output_index = ?"
                )?,
                stmt_update_sent_note: self.conn.prepare(
                    "UPDATE sent_notes
                    SET from_account = ?, address = ?, value = ?, memo = ?
                    WHERE tx = ? AND output_index = ?",
                )?,
                stmt_insert_sent_note: self.conn.prepare(
                    "INSERT INTO sent_notes (tx, output_index, from_account, address, value, memo)
                    VALUES (?, ?, ?, ?, ?, ?)",
                )?,
                stmt_insert_witness: self.conn.prepare(
                    "INSERT INTO sapling_witnesses (note, block, witness)
                    VALUES (?, ?, ?)",
                )?,
                stmt_prune_witnesses: self.conn.prepare(
                    "DELETE FROM sapling_witnesses WHERE block < ?"
                )?,
                stmt_update_expired: self.conn.prepare(
                    "UPDATE received_notes SET spent = NULL WHERE EXISTS (
                        SELECT id_tx FROM transactions
                        WHERE id_tx = received_notes.spent AND block IS NULL AND expiry_height < ?
                    )",
                )?,
            }
        )
    }
}

impl<P: consensus::Parameters> WalletRead for WalletDb<P> {
    type Error = SqliteClientError;
    type NoteRef = NoteId;
    type TxRef = i64;

    fn block_height_extrema(&self) -> Result<Option<(BlockHeight, BlockHeight)>, Self::Error> {
        wallet::block_height_extrema(self).map_err(SqliteClientError::from)
    }

    fn get_block_hash(&self, block_height: BlockHeight) -> Result<Option<BlockHash>, Self::Error> {
        wallet::get_block_hash(self, block_height).map_err(SqliteClientError::from)
    }

    fn get_tx_height(&self, txid: TxId) -> Result<Option<BlockHeight>, Self::Error> {
        wallet::get_tx_height(self, txid).map_err(SqliteClientError::from)
    }

    fn get_extended_full_viewing_keys(
        &self,
    ) -> Result<HashMap<AccountId, ExtendedFullViewingKey>, Self::Error> {
        wallet::get_extended_full_viewing_keys(self)
    }

    fn get_address(&self, account: AccountId) -> Result<Option<PaymentAddress>, Self::Error> {
        wallet::get_address(self, account)
    }

    fn is_valid_account_extfvk(
        &self,
        account: AccountId,
        extfvk: &ExtendedFullViewingKey,
    ) -> Result<bool, Self::Error> {
        wallet::is_valid_account_extfvk(self, account, extfvk)
    }

    fn get_balance_at(
        &self,
        account: AccountId,
        anchor_height: BlockHeight,
    ) -> Result<Amount, Self::Error> {
        wallet::get_balance_at(self, account, anchor_height)
    }

    fn get_memo(&self, id_note: Self::NoteRef) -> Result<Memo, Self::Error> {
        match id_note {
            NoteId::SentNoteId(id_note) => wallet::get_sent_memo(self, id_note),
            NoteId::ReceivedNoteId(id_note) => wallet::get_received_memo(self, id_note),
        }
    }

    fn get_commitment_tree(
        &self,
        block_height: BlockHeight,
    ) -> Result<Option<CommitmentTree<Node>>, Self::Error> {
        wallet::get_commitment_tree(self, block_height)
    }

    #[allow(clippy::type_complexity)]
    fn get_witnesses(
        &self,
        block_height: BlockHeight,
    ) -> Result<Vec<(Self::NoteRef, IncrementalWitness<Node>)>, Self::Error> {
        wallet::get_witnesses(self, block_height)
    }

    fn get_nullifiers(&self) -> Result<Vec<(AccountId, Nullifier)>, Self::Error> {
        wallet::get_nullifiers(self)
    }

    fn get_spendable_notes(
        &self,
        account: AccountId,
        anchor_height: BlockHeight,
    ) -> Result<Vec<SpendableNote>, Self::Error> {
        wallet::transact::get_spendable_notes(self, account, anchor_height)
    }

    fn select_spendable_notes(
        &self,
        account: AccountId,
        target_value: Amount,
        anchor_height: BlockHeight,
    ) -> Result<Vec<SpendableNote>, Self::Error> {
        wallet::transact::select_spendable_notes(self, account, target_value, anchor_height)
    }
}

/// The primary type used to implement [`WalletWrite`] for the SQLite database.
///
/// A data structure that stores the SQLite prepared statements that are
/// required for the implementation of [`WalletWrite`] against the backing
/// store.
///
/// [`WalletWrite`]: zcash_client_backend::data_api::WalletWrite
pub struct DataConnStmtCache<'a, P> {
    wallet_db: &'a WalletDb<P>,
    stmt_insert_block: Statement<'a>,

    stmt_insert_tx_meta: Statement<'a>,
    stmt_update_tx_meta: Statement<'a>,

    stmt_insert_tx_data: Statement<'a>,
    stmt_update_tx_data: Statement<'a>,
    stmt_select_tx_ref: Statement<'a>,

    stmt_mark_recived_note_spent: Statement<'a>,

    stmt_insert_received_note: Statement<'a>,
    stmt_update_received_note: Statement<'a>,
    stmt_select_received_note: Statement<'a>,

    stmt_insert_sent_note: Statement<'a>,
    stmt_update_sent_note: Statement<'a>,

    stmt_insert_witness: Statement<'a>,
    stmt_prune_witnesses: Statement<'a>,
    stmt_update_expired: Statement<'a>,
}

impl<'a, P: consensus::Parameters> WalletRead for DataConnStmtCache<'a, P> {
    type Error = SqliteClientError;
    type NoteRef = NoteId;
    type TxRef = i64;

    fn block_height_extrema(&self) -> Result<Option<(BlockHeight, BlockHeight)>, Self::Error> {
        self.wallet_db.block_height_extrema()
    }

    fn get_block_hash(&self, block_height: BlockHeight) -> Result<Option<BlockHash>, Self::Error> {
        self.wallet_db.get_block_hash(block_height)
    }

    fn get_tx_height(&self, txid: TxId) -> Result<Option<BlockHeight>, Self::Error> {
        self.wallet_db.get_tx_height(txid)
    }

    fn get_extended_full_viewing_keys(
        &self,
    ) -> Result<HashMap<AccountId, ExtendedFullViewingKey>, Self::Error> {
        self.wallet_db.get_extended_full_viewing_keys()
    }

    fn get_address(&self, account: AccountId) -> Result<Option<PaymentAddress>, Self::Error> {
        self.wallet_db.get_address(account)
    }

    fn is_valid_account_extfvk(
        &self,
        account: AccountId,
        extfvk: &ExtendedFullViewingKey,
    ) -> Result<bool, Self::Error> {
        self.wallet_db.is_valid_account_extfvk(account, extfvk)
    }

    fn get_balance_at(
        &self,
        account: AccountId,
        anchor_height: BlockHeight,
    ) -> Result<Amount, Self::Error> {
        self.wallet_db.get_balance_at(account, anchor_height)
    }

    fn get_memo(&self, id_note: Self::NoteRef) -> Result<Memo, Self::Error> {
        self.wallet_db.get_memo(id_note)
    }

    fn get_commitment_tree(
        &self,
        block_height: BlockHeight,
    ) -> Result<Option<CommitmentTree<Node>>, Self::Error> {
        self.wallet_db.get_commitment_tree(block_height)
    }

    #[allow(clippy::type_complexity)]
    fn get_witnesses(
        &self,
        block_height: BlockHeight,
    ) -> Result<Vec<(Self::NoteRef, IncrementalWitness<Node>)>, Self::Error> {
        self.wallet_db.get_witnesses(block_height)
    }

    fn get_nullifiers(&self) -> Result<Vec<(AccountId, Nullifier)>, Self::Error> {
        self.wallet_db.get_nullifiers()
    }

    fn get_spendable_notes(
        &self,
        account: AccountId,
        anchor_height: BlockHeight,
    ) -> Result<Vec<SpendableNote>, Self::Error> {
        self.wallet_db.get_spendable_notes(account, anchor_height)
    }

    fn select_spendable_notes(
        &self,
        account: AccountId,
        target_value: Amount,
        anchor_height: BlockHeight,
    ) -> Result<Vec<SpendableNote>, Self::Error> {
        self.wallet_db
            .select_spendable_notes(account, target_value, anchor_height)
    }
}

impl<'a, P: consensus::Parameters> DataConnStmtCache<'a, P> {
    fn transactionally<F, A>(&mut self, f: F) -> Result<A, SqliteClientError>
    where
        F: FnOnce(&mut Self) -> Result<A, SqliteClientError>,
    {
        self.wallet_db.conn.execute("BEGIN IMMEDIATE", [])?;
        match f(self) {
            Ok(result) => {
                self.wallet_db.conn.execute("COMMIT", [])?;
                Ok(result)
            }
            Err(error) => {
                match self.wallet_db.conn.execute("ROLLBACK", []) {
                    Ok(_) => Err(error),
                    Err(e) =>
                        // Panicking here is probably the right thing to do, because it
                        // means the database is corrupt.
                        panic!(
                            "Rollback failed with error {} while attempting to recover from error {}; database is likely corrupt.",
                            e,
                            error
                        )
                }
            }
        }
    }
}

impl<'a, P: consensus::Parameters> WalletWrite for DataConnStmtCache<'a, P> {
    #[allow(clippy::type_complexity)]
    fn advance_by_block(
        &mut self,
        block: &PrunedBlock,
        updated_witnesses: &[(Self::NoteRef, IncrementalWitness<Node>)],
    ) -> Result<Vec<(Self::NoteRef, IncrementalWitness<Node>)>, Self::Error> {
        // database updates for each block are transactional
        self.transactionally(|up| {
            // Insert the block into the database.
            wallet::insert_block(
                up,
                block.block_height,
                block.block_hash,
                block.block_time,
                &block.commitment_tree,
            )?;

            let mut new_witnesses = vec![];
            for tx in block.transactions {
                let tx_row = wallet::put_tx_meta(up, &tx, block.block_height)?;

                // Mark notes as spent and remove them from the scanning cache
                for spend in &tx.shielded_spends {
                    wallet::mark_spent(up, tx_row, &spend.nf)?;
                }

                for output in &tx.shielded_outputs {
                    let received_note_id = wallet::put_received_note(up, output, tx_row)?;

                    // Save witness for note.
                    new_witnesses.push((received_note_id, output.witness.clone()));
                }
            }

            // Insert current new_witnesses into the database.
            for (received_note_id, witness) in updated_witnesses.iter().chain(new_witnesses.iter())
            {
                if let NoteId::ReceivedNoteId(rnid) = *received_note_id {
                    wallet::insert_witness(up, rnid, witness, block.block_height)?;
                } else {
                    return Err(SqliteClientError::InvalidNoteId);
                }
            }

            // Prune the stored witnesses (we only expect rollbacks of at most 100 blocks).
            let below_height = if block.block_height < BlockHeight::from(100) {
                BlockHeight::from(0)
            } else {
                block.block_height - 100
            };
            wallet::prune_witnesses(up, below_height)?;

            // Update now-expired transactions that didn't get mined.
            wallet::update_expired_notes(up, block.block_height)?;

            Ok(new_witnesses)
        })
    }

    fn store_received_tx(
        &mut self,
        received_tx: &ReceivedTransaction,
    ) -> Result<Self::TxRef, Self::Error> {
        self.transactionally(|up| {
            let tx_ref = wallet::put_tx_data(up, received_tx.tx, None)?;

            for output in received_tx.outputs {
                if output.outgoing {
                    wallet::put_sent_note(up, output, tx_ref)?;
                } else {
                    wallet::put_received_note(up, output, tx_ref)?;
                }
            }

            Ok(tx_ref)
        })
    }

    fn store_sent_tx(&mut self, sent_tx: &SentTransaction) -> Result<Self::TxRef, Self::Error> {
        // Update the database atomically, to ensure the result is internally consistent.
        self.transactionally(|up| {
            let tx_ref = wallet::put_tx_data(up, &sent_tx.tx, Some(sent_tx.created))?;

            // Mark notes as spent.
            //
            // This locks the notes so they aren't selected again by a subsequent call to
            // create_spend_to_address() before this transaction has been mined (at which point the notes
            // get re-marked as spent).
            //
            // Assumes that create_spend_to_address() will never be called in parallel, which is a
            // reasonable assumption for a light client such as a mobile phone.
            for spend in &sent_tx.tx.shielded_spends {
                wallet::mark_spent(up, tx_ref, &spend.nullifier)?;
            }

            wallet::insert_sent_note(
                up,
                tx_ref,
                sent_tx.output_index,
                sent_tx.account,
                sent_tx.recipient_address,
                sent_tx.value,
                sent_tx.memo.as_ref(),
            )?;

            // Return the row number of the transaction, so the caller can fetch it for sending.
            Ok(tx_ref)
        })
    }

    fn rewind_to_height(&mut self, block_height: BlockHeight) -> Result<(), Self::Error> {
        wallet::rewind_to_height(self.wallet_db, block_height)
    }
}

/// A wrapper for the SQLite connection to the block cache database.
pub struct BlockDb(Connection);

impl BlockDb {
    /// Opens a connection to the wallet database stored at the specified path.
    pub fn for_path<P: AsRef<Path>>(path: P) -> Result<Self, rusqlite::Error> {
        Connection::open(path).map(BlockDb)
    }
}

impl BlockSource for BlockDb {
    type Error = SqliteClientError;

    fn with_blocks<F>(
        &self,
        from_height: BlockHeight,
        limit: Option<u32>,
        with_row: F,
    ) -> Result<(), Self::Error>
    where
        F: FnMut(CompactBlock) -> Result<(), Self::Error>,
    {
        chain::with_blocks(self, from_height, limit, with_row)
    }
}

fn address_from_extfvk<P: consensus::Parameters>(
    params: &P,
    extfvk: &ExtendedFullViewingKey,
) -> String {
    let addr = extfvk.default_address().unwrap().1;
    encode_payment_address(params.hrp_sapling_payment_address(), &addr)
}

#[cfg(test)]
mod tests {
    use protobuf::Message;
    use rusqlite::params;

    use zcash_client_backend::proto::compact_formats::CompactBlock;

    use zcash_primitives::consensus::{BlockHeight, Network, NetworkUpgrade, Parameters};

    use super::BlockDb;

    #[cfg(feature = "mainnet")]
    pub(crate) fn network() -> Network {
        Network::MainNetwork
    }

    #[cfg(not(feature = "mainnet"))]
    pub(crate) fn network() -> Network {
        Network::TestNetwork
    }

    #[cfg(feature = "mainnet")]
    pub(crate) fn sapling_activation_height() -> BlockHeight {
        Network::MainNetwork
            .activation_height(NetworkUpgrade::Sapling)
            .unwrap()
    }

    #[cfg(not(feature = "mainnet"))]
    pub(crate) fn sapling_activation_height() -> BlockHeight {
        Network::TestNetwork
            .activation_height(NetworkUpgrade::Sapling)
            .unwrap()
    }

    /// Insert a fake CompactBlock into the cache DB.
    pub(crate) fn insert_into_cache(db_cache: &BlockDb, cb: &CompactBlock) {
        let cb_bytes = cb.write_to_bytes().unwrap();
        db_cache
            .0
            .prepare("INSERT INTO compactblocks (height, data) VALUES (?, ?)")
            .unwrap()
            .execute(params![u32::from(cb.height()), cb_bytes,])
            .unwrap();
    }
}
