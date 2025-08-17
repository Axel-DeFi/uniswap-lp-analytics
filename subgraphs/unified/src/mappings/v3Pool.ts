import { Address, BigInt, BigDecimal } from "@graphprotocol/graph-ts";
import { Swap } from "../../generated/templates/UniswapV3Pool/UniswapV3Pool";
import { ERC20 } from "../../generated/UniswapV3Factory/ERC20";
import { Pool, Token, PoolDayData, PoolHourData, PoolPriceHour } from "../../generated/schema";

const USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7";
function isStable(addr: string): boolean {
  const a = addr.toLowerCase();
  return a == USDC || a == USDT;
}
function safeBalanceOf(tokenAddr: string, owner: Address): BigInt {
  const erc = ERC20.bind(Address.fromString(tokenAddr));
  return erc.balanceOf(owner);
}

function exponentToBigDecimal(decimals: i32): BigDecimal {
  let result = BigDecimal.fromString("1");
  const ten = BigDecimal.fromString("10");
  for (let i = 0; i < decimals; i++) {
    result = result.times(ten);
  }
  return result;
}

function hourId(ts: BigInt): i32 { return ts.toI32() / 3600; }
function dayId(ts: BigInt): i32 { return ts.toI32() / 86400; }

function ensurePoolDayData(poolId: string, ts: BigInt): PoolDayData {
  const id = poolId + "-" + dayId(ts).toString();
  let p = PoolDayData.load(id);
  if (p == null) {
    p = new PoolDayData(id);
    p.pool = poolId;
    p.date = dayId(ts);
    p.volumeToken0 = BigDecimal.fromString("0");
    p.volumeToken1 = BigDecimal.fromString("0");
    p.swapCount = 0;
    p.volumeUSD = BigDecimal.fromString("0");
    p.feesUSD = BigDecimal.fromString("0");
    p.tvlUSD = BigDecimal.fromString("0");
  }
  return p as PoolDayData;
}

function ensurePoolHourData(poolId: string, ts: BigInt): PoolHourData {
  const id = poolId + "-" + hourId(ts).toString();
  let p = PoolHourData.load(id);
  if (p == null) {
    p = new PoolHourData(id);
    p.pool = poolId;
    p.hourStartUnix = hourId(ts);
    p.volumeToken0 = BigDecimal.fromString("0");
    p.volumeToken1 = BigDecimal.fromString("0");
    p.swapCount = 0;
    p.volumeUSD = BigDecimal.fromString("0");
    p.feesUSD = BigDecimal.fromString("0");
    p.tvlUSD = BigDecimal.fromString("0");
  }
  return p as PoolHourData;
}

function ensurePriceHour(poolId: string, ts: BigInt): PoolPriceHour {
  const id = poolId + "-" + hourId(ts).toString();
  let ph = PoolPriceHour.load(id);
  if (ph == null) {
    ph = new PoolPriceHour(id);
    ph.pool = poolId;
    ph.hourStartUnix = hourId(ts);
    ph.sqrtPriceX96 = BigInt.zero();
    ph.price0 = BigDecimal.fromString("0");
    ph.price1 = BigDecimal.fromString("0");
    ph.liquidity = BigInt.zero();
    ph.updatedAt = 0;
  }
  return ph as PoolPriceHour;
}

function absBigInt(x: BigInt): BigInt {
  return x.ge(BigInt.zero()) ? x : x.times(BigInt.fromI32(-1));
}

// Convert sqrtPriceX96 to token1 per 1 token0 and the inverse, decimals-adjusted.
function pricesFromSqrt(
  sqrtPriceX96: BigInt,
  token0Decimals: i32,
  token1Decimals: i32
): BigDecimal[] {
  const Q192 = BigInt.fromI32(2).pow(192);
  const num = sqrtPriceX96.times(sqrtPriceX96).toBigDecimal();
  const denom = Q192.toBigDecimal();
  const ratio = num.div(denom); // raw token1 per token0 (no decimals adjust)
  const adj = exponentToBigDecimal(token0Decimals).div(exponentToBigDecimal(token1Decimals));
  const p0 = ratio.times(adj); // token1 for 1 token0
  if (p0.equals(BigDecimal.fromString("0"))) {
    return [BigDecimal.fromString("0"), BigDecimal.fromString("0")];
  }
  const p1 = BigDecimal.fromString("1").div(p0); // token0 for 1 token1
  return [p0, p1];
}

export function handleSwap(event: Swap): void {
  const poolId = event.address.toHex().toLowerCase();
  let pool = Pool.load(poolId);
  if (pool == null) {
    return;
  }

  let day = ensurePoolDayData(poolId, event.block.timestamp);
  let hour = ensurePoolHourData(poolId, event.block.timestamp);

  const v0 = absBigInt(event.params.amount0).toBigDecimal();
  const v1 = absBigInt(event.params.amount1).toBigDecimal();

  const t0 = Token.load(pool.token0);
  const t1 = Token.load(pool.token1);
  if (t0 == null || t1 == null) return;

  const d0 = exponentToBigDecimal(t0.decimals);
  const d1 = exponentToBigDecimal(t1.decimals);

  day.volumeToken0 = day.volumeToken0.plus(v0.div(d0));
  day.volumeToken1 = day.volumeToken1.plus(v1.div(d1));
  day.swapCount = day.swapCount + 1;
  day.save();

  hour.volumeToken0 = hour.volumeToken0.plus(v0.div(d0));
  hour.volumeToken1 = hour.volumeToken1.plus(v1.div(d1));
  hour.swapCount = hour.swapCount + 1;
  hour.save();
  let ph = ensurePriceHour(poolId, event.block.timestamp);
  ph.sqrtPriceX96 = event.params.sqrtPriceX96;
  ph.price0 = p0;
  ph.price1 = p1;
  ph.liquidity = event.params.liquidity;
  ph.updatedAt = event.block.timestamp.toI32();
  ph.save();
const poolAddr = event.address;
const bal0Raw = safeBalanceOf(pool.token0, poolAddr);
const bal1Raw = safeBalanceOf(pool.token1, poolAddr);
const amt0 = bal0Raw.toBigDecimal().div(exponentToBigDecimal(t0.decimals));
const amt1 = bal1Raw.toBigDecimal().div(exponentToBigDecimal(t1.decimals));
let tvlUSD = BigDecimal.fromString("0");
if (isStable(pool.token0) && !isStable(pool.token1)) {
  tvlUSD = amt0.plus(amt1.times(p1));
} else if (isStable(pool.token1) && !isStable(pool.token0)) {
  tvlUSD = amt1.plus(amt0.times(p0));
}
day.tvlUSD = tvlUSD;
day.save();
hour.tvlUSD = tvlUSD;
hour.save();
}
