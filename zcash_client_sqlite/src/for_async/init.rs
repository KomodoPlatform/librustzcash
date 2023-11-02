use crate::error::SqliteClientError;
use crate::for_async::{async_blocking, WalletDbAsync};
use crate::wallet;
use zcash_primitives::block::BlockHash;
use zcash_primitives::consensus;
use zcash_primitives::consensus::BlockHeight;
use zcash_primitives::zip32::ExtendedFullViewingKey;

pub async fn init_wallet_db<P: consensus::Parameters + 'static>(
    wdb: &WalletDbAsync<P>,
) -> Result<(), rusqlite::Error>
where
    P: Clone + Send + Sync,
{
    let wdb = wdb.inner();
    async_blocking(move || {
        let wdb = wdb.lock().unwrap();
        wallet::init::init_wallet_db(&wdb)
    })
    .await
}

pub async fn init_accounts_table<P: consensus::Parameters + 'static>(
    wdb: &WalletDbAsync<P>,
    extfvks: &[ExtendedFullViewingKey],
) -> Result<(), SqliteClientError>
where
    P: Clone + Send + Sync,
{
    let wdb = wdb.inner();
    let extfvks = extfvks.to_vec();
    async_blocking(move || {
        let wdb = wdb.lock().unwrap();
        wallet::init::init_accounts_table(&wdb, &extfvks)
    })
    .await
}

pub async fn init_blocks_table<P: consensus::Parameters + 'static>(
    wdb: &WalletDbAsync<P>,
    height: BlockHeight,
    hash: BlockHash,
    time: u32,
    sapling_tree: &[u8],
) -> Result<(), SqliteClientError>
where
    P: Clone + Send + Sync,
{
    let wdb = wdb.inner();
    let sapling_tree = sapling_tree.to_vec();
    async_blocking(move || {
        let wdb = wdb.lock().unwrap();
        wallet::init::init_blocks_table(&wdb, height, hash, time, &sapling_tree)
    })
    .await
}
