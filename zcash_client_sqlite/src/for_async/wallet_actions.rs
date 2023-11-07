use crate::error::SqliteClientError;
use crate::{wallet, NoteId, WalletDb};
use std::sync::MutexGuard;
use zcash_client_backend::address::RecipientAddress;
use zcash_client_backend::wallet::{AccountId, WalletTx};
use zcash_client_backend::DecryptedOutput;
use zcash_extras::ShieldedOutput;
use zcash_primitives::block::BlockHash;
use zcash_primitives::consensus::{BlockHeight, Parameters};
use zcash_primitives::memo::MemoBytes;
use zcash_primitives::merkle_tree::{CommitmentTree, IncrementalWitness};
use zcash_primitives::sapling::{Node, Nullifier};
use zcash_primitives::transaction::components::Amount;
use zcash_primitives::transaction::Transaction;

pub fn insert_block<P: Parameters>(
    db: &MutexGuard<WalletDb<P>>,
    block_height: BlockHeight,
    block_hash: BlockHash,
    block_time: u32,
    commitment_tree: &CommitmentTree<Node>,
) -> Result<(), SqliteClientError> {
    let mut update_ops = db.get_update_ops()?;
    wallet::insert_block(
        &mut update_ops,
        block_height,
        block_hash,
        block_time,
        commitment_tree,
    )
}

pub fn put_tx_meta<P: Parameters, N>(
    db: &MutexGuard<WalletDb<P>>,
    tx: &WalletTx<N>,
    height: BlockHeight,
) -> Result<i64, SqliteClientError> {
    let mut update_ops = db.get_update_ops()?;
    wallet::put_tx_meta(&mut update_ops, tx, height)
}

pub fn mark_spent<P: Parameters>(
    db: &MutexGuard<WalletDb<P>>,
    tx_ref: i64,
    nf: &Nullifier,
) -> Result<(), SqliteClientError> {
    wallet::mark_spent(&mut db.get_update_ops()?, tx_ref, nf)
}

pub fn put_received_note<P: Parameters, T: ShieldedOutput>(
    db: &MutexGuard<WalletDb<P>>,
    output: &T,
    tx_ref: i64,
) -> Result<NoteId, SqliteClientError> {
    let mut update_ops = db.get_update_ops()?;
    wallet::put_received_note(&mut update_ops, output, tx_ref)
}

pub fn insert_witness<P: Parameters>(
    db: &MutexGuard<WalletDb<P>>,
    note_id: i64,
    witness: &IncrementalWitness<Node>,
    height: BlockHeight,
) -> Result<(), SqliteClientError> {
    let mut update_ops = db.get_update_ops()?;
    wallet::insert_witness(&mut update_ops, note_id, witness, height)
}

pub fn prune_witnesses<P: Parameters>(
    db: &MutexGuard<WalletDb<P>>,
    below_height: BlockHeight,
) -> Result<(), SqliteClientError> {
    let mut update_ops = db.get_update_ops()?;
    wallet::prune_witnesses(&mut update_ops, below_height)
}

pub fn update_expired_notes<P: Parameters>(
    db: &MutexGuard<WalletDb<P>>,
    height: BlockHeight,
) -> Result<(), SqliteClientError> {
    let mut update_ops = db.get_update_ops()?;
    wallet::update_expired_notes(&mut update_ops, height)
}

pub fn put_tx_data<P: Parameters>(
    db: &MutexGuard<WalletDb<P>>,
    tx: &Transaction,
    created_at: Option<time::OffsetDateTime>,
) -> Result<i64, SqliteClientError> {
    let mut update_ops = db.get_update_ops()?;
    wallet::put_tx_data(&mut update_ops, tx, created_at)
}

pub fn put_sent_note<P: Parameters>(
    db: &MutexGuard<WalletDb<P>>,
    output: &DecryptedOutput,
    tx_ref: i64,
) -> Result<(), SqliteClientError> {
    let mut update_ops = db.get_update_ops()?;
    wallet::put_sent_note(&mut update_ops, output, tx_ref)
}

pub fn insert_sent_note<P: Parameters>(
    db: &MutexGuard<WalletDb<P>>,
    tx_ref: i64,
    output_index: usize,
    account: AccountId,
    to: &RecipientAddress,
    value: Amount,
    memo: Option<&MemoBytes>,
) -> Result<(), SqliteClientError> {
    let mut update_ops = db.get_update_ops()?;
    wallet::insert_sent_note(
        &mut update_ops,
        tx_ref,
        output_index,
        account,
        to,
        value,
        memo,
    )
}
