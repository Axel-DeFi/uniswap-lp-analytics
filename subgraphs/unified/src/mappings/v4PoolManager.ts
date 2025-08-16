import { Address, BigInt, BigDecimal } from "@graphprotocol/graph-ts";
import { Pool, Token, PoolDayData, PoolHourData } from "../../generated/schema";
import { Initialize } from "../../generated/PoolManager/PoolManager";
import { ERC20 } from "../../generated/PoolManager/ERC20";

function getOrCreateToken(addr: Address, chainId: i32, ts: BigInt): string {
  const id = addr.toHex().toLowerCase();
  let token = Token.load(id);
  if (token == null) {
    token = new Token(id);
    const erc20 = ERC20.bind(addr);

    const dec = erc20.try_decimals();
    token.decimals = dec.reverted ? 18 : (dec.value as i32);

    const sym = erc20.try_symbol();
    token.symbol = sym.reverted ? null : sym.value;

    const nam = erc20.try_name();
    token.name = nam.reverted ? null : nam.value;

    token.chainId = chainId;
    token.createdAtTimestamp = ts;
    token.save();
  }
  return id;
}

function ensurePoolDayData(poolId: string, ts: BigInt): void {
  const dayId = ts.toI32() / 86400; // UTC day
  const id = poolId + "-" + dayId.toString();
  let pdd = PoolDayData.load(id);
  if (pdd == null) {
    pdd = new PoolDayData(id);
    pdd.pool = poolId;
    pdd.date = dayId;
    pdd.volumeUSD = BigDecimal.fromString("0");
    pdd.feesUSD = BigDecimal.fromString("0");
    pdd.tvlUSD = BigDecimal.fromString("0");
    pdd.save();
  }
}

function ensurePoolHourData(poolId: string, ts: BigInt): void {
  const hourId = ts.toI32() / 3600; // UTC hour
  const id = poolId + "-" + hourId.toString();
  let phd = PoolHourData.load(id);
  if (phd == null) {
    phd = new PoolHourData(id);
    phd.pool = poolId;
    phd.hourStartUnix = hourId;
    phd.volumeUSD = BigDecimal.fromString("0");
    phd.feesUSD = BigDecimal.fromString("0");
    phd.tvlUSD = BigDecimal.fromString("0");
    phd.save();
  }
}

export function handleInitialize(event: Initialize): void {
  const poolId = event.params.id.toHex().toLowerCase();

  const t0 = getOrCreateToken(event.params.currency0 as Address, 1, event.block.timestamp);
  const t1 = getOrCreateToken(event.params.currency1 as Address, 1, event.block.timestamp);

  let pool = new Pool(poolId);
  pool.version = 4;
  pool.chainId = 1; // Ethereum mainnet in this step
  pool.token0 = t0;
  pool.token1 = t1;
  pool.feeTierBps = event.params.fee as i32;
  pool.tickSpacing = event.params.tickSpacing as i32;
  pool.createdAtTimestamp = event.block.timestamp;
  pool.save();

  ensurePoolDayData(poolId, event.block.timestamp);
  ensurePoolHourData(poolId, event.block.timestamp);
}
