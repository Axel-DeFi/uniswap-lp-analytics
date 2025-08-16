import { Address, BigDecimal, BigInt } from "@graphprotocol/graph-ts";
import { Pool, PoolDayData, PoolHourData, Token } from "../../generated/schema";
import { Swap } from "../../generated/templates/UniswapV3Pool/UniswapV3Pool";

function absBigInt(x: BigInt): BigInt {
  return x.lt(BigInt.zero()) ? x.times(BigInt.fromI32(-1)) : x;
}

function pow10BD(decimals: i32): BigDecimal {
  let result = BigDecimal.fromString("1");
  for (let i = 0; i < decimals; i++) {
    result = result.times(BigDecimal.fromString("10"));
  }
  return result;
}

function toDecimal(amount: BigInt, decimals: i32): BigDecimal {
  // Convert BigInt to BigDecimal and scale by 10^decimals
  const bdAmount = BigDecimal.fromString(amount.toString());
  return bdAmount.div(pow10BD(decimals));
}

function ensureDay(poolId: string, ts: BigInt): PoolDayData {
  const dayId = ts.toI32() / 86400;
  const id = poolId + "-" + dayId.toString();
  let ent = PoolDayData.load(id);
  if (ent == null) {
    ent = new PoolDayData(id);
    ent.pool = poolId;
    ent.date = dayId;
    ent.volumeToken0 = BigDecimal.fromString("0");
    ent.volumeToken1 = BigDecimal.fromString("0");
    ent.swapCount = 0;
    ent.volumeUSD = BigDecimal.fromString("0");
    ent.feesUSD = BigDecimal.fromString("0");
    ent.tvlUSD = BigDecimal.fromString("0");
  }
  return ent as PoolDayData;
}

function ensureHour(poolId: string, ts: BigInt): PoolHourData {
  const hourId = ts.toI32() / 3600;
  const id = poolId + "-" + hourId.toString();
  let ent = PoolHourData.load(id);
  if (ent == null) {
    ent = new PoolHourData(id);
    ent.pool = poolId;
    ent.hourStartUnix = hourId;
    ent.volumeToken0 = BigDecimal.fromString("0");
    ent.volumeToken1 = BigDecimal.fromString("0");
    ent.swapCount = 0;
    ent.volumeUSD = BigDecimal.fromString("0");
    ent.feesUSD = BigDecimal.fromString("0");
    ent.tvlUSD = BigDecimal.fromString("0");
  }
  return ent as PoolHourData;
}

export function handleSwap(event: Swap): void {
  const poolId = event.address.toHex().toLowerCase();
  const pool = Pool.load(poolId);
  if (pool == null) return;

  const token0 = Token.load(pool.token0);
  const token1 = Token.load(pool.token1);
  if (token0 == null || token1 == null) return;

  // Absolute amounts in token units
  const a0Abs = absBigInt(event.params.amount0);
  const a1Abs = absBigInt(event.params.amount1);
  const amt0 = toDecimal(a0Abs, token0.decimals);
  const amt1 = toDecimal(a1Abs, token1.decimals);

  // Update daily/hourly aggregates
  const day = ensureDay(poolId, event.block.timestamp);
  const hour = ensureHour(poolId, event.block.timestamp);

  day.volumeToken0 = day.volumeToken0.plus(amt0);
  day.volumeToken1 = day.volumeToken1.plus(amt1);
  day.swapCount = day.swapCount + 1;
  day.save();

  hour.volumeToken0 = hour.volumeToken0.plus(amt0);
  hour.volumeToken1 = hour.volumeToken1.plus(amt1);
  hour.swapCount = hour.swapCount + 1;
  hour.save();
}
