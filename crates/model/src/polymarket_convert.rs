// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Cost and profit/loss allocation for a confirmed Polymarket Convert.

use nautilus_core::correctness::{CorrectnessError, CorrectnessResult};
use ruint::aliases::U512;
use rust_decimal::Decimal;

use crate::{
    identifiers::InstrumentId,
    position::from_raw,
    types::{Currency, fixed::check_fixed_precision},
};

/// A consumed NO instrument and the cost released by the caller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PolymarketConvertInput {
    /// The consumed NO instrument.
    pub instrument_id: InstrumentId,
    /// The positive quantity consumed.
    pub quantity: Decimal,
    /// The nonnegative released cost in the settlement currency.
    pub released_cost: Decimal,
}

/// A received YES instrument and its net quantity after conversion fees.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PolymarketConvertOutput {
    /// The received YES instrument.
    pub instrument_id: InstrumentId,
    /// The nonnegative quantity received.
    pub quantity: Decimal,
}

/// The per-instrument accounting allocations in the settlement currency.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolymarketConvertAllocation {
    /// Immediate realized profit or loss for every input, ordered by instrument ID.
    pub input_pnl: Vec<(InstrumentId, Decimal)>,
    /// Carryover cost for every positive-quantity output, ordered by instrument ID.
    pub output_costs: Vec<(InstrumentId, Decimal)>,
}

/// Allocates the cost of consumed NO instruments to net cash and received YES instruments.
///
/// With positive YES outputs, unrecovered input cost is carried over to those outputs and any
/// excess cash is realized as profit. Without positive YES outputs, cash minus input cost is
/// realized immediately. The supplied cash and quantities already reflect conversion fees.
///
/// Output costs are weighted by received quantity. Input PnL is weighted by released cost, or by
/// consumed quantity if the total released cost is zero. In instrument ID order, each share's
/// magnitude is rounded down to the settlement currency precision, with the final positive-weight
/// instrument receiving the remainder. Zero-quantity outputs are ignored.
///
/// Allocated totals and shares must be exactly representable as `Decimal` at the settlement
/// currency precision.
///
/// # Errors
///
/// Returns an error if:
/// - Inputs are empty or either allocated side contains duplicate instrument IDs.
/// - An input quantity is nonpositive, an output quantity is negative, or cash or released cost
///   is negative.
/// - The settlement currency precision is invalid or an amount cannot be represented exactly
///   as `Decimal` at that precision.
pub fn allocate_convert_costs(
    inputs: &[PolymarketConvertInput],
    outputs: &[PolymarketConvertOutput],
    net_cash: Decimal,
    settlement_currency: Currency,
) -> CorrectnessResult<PolymarketConvertAllocation> {
    allocate_convert_costs_inner(inputs, outputs, net_cash, settlement_currency, false)
}

/// Allocates released costs that may carry fractional settlement-currency units.
///
/// This applies the same cost and PnL relationships as [`allocate_convert_costs`], but accepts
/// fractional released costs from a proportional position reduction without first rounding them
/// to the settlement currency precision. Every non-final share is rounded down at the maximum
/// scale needed by the released costs, net cash, and settlement currency precision, and the final
/// positive-weight share receives the exact remainder.
///
/// # Errors
///
/// Returns an error if:
/// - Inputs are empty or either allocated side contains duplicate instrument IDs.
/// - An input quantity is nonpositive, an output quantity is negative, or cash or released cost
///   is negative.
/// - The settlement currency precision is invalid or a final allocated leg cannot be represented
///   exactly as `Decimal`.
pub fn allocate_convert_costs_precise(
    inputs: &[PolymarketConvertInput],
    outputs: &[PolymarketConvertOutput],
    net_cash: Decimal,
    settlement_currency: Currency,
) -> CorrectnessResult<PolymarketConvertAllocation> {
    allocate_convert_costs_inner(inputs, outputs, net_cash, settlement_currency, true)
}

fn allocate_convert_costs_inner(
    inputs: &[PolymarketConvertInput],
    outputs: &[PolymarketConvertOutput],
    net_cash: Decimal,
    settlement_currency: Currency,
    precise: bool,
) -> CorrectnessResult<PolymarketConvertAllocation> {
    if inputs.is_empty() {
        return Err(invalid("`inputs` was empty"));
    }

    let currency_precision = settlement_currency.precision;
    check_fixed_precision(currency_precision)?;
    let precision = if precise {
        inputs
            .iter()
            .map(|input| input.released_cost.normalize().scale())
            .chain(std::iter::once(net_cash.normalize().scale()))
            .max()
            .unwrap_or(0)
            .max(u32::from(currency_precision))
    } else {
        u32::from(currency_precision)
    };

    let mut legs = Vec::with_capacity(inputs.len());
    for input in inputs {
        if input.released_cost < Decimal::ZERO {
            return Err(invalid(format!(
                "input `{}` released cost cannot be negative, was {}",
                input.instrument_id, input.released_cost
            )));
        }

        if input.quantity <= Decimal::ZERO {
            return Err(invalid(format!(
                "input `{}` quantity was not positive, was {}",
                input.instrument_id, input.quantity
            )));
        }
        let cost_units = amount_to_units(
            input.released_cost,
            precision,
            &format!("input `{}` released cost", input.instrument_id),
        )?;
        legs.push((
            input.instrument_id,
            input.quantity,
            input.released_cost,
            cost_units,
        ));
    }
    legs.sort_unstable_by_key(|leg| leg.0);
    check_duplicate_instruments(legs.windows(2).map(|pair| (pair[0].0, pair[1].0)), "input")?;

    for output in outputs {
        if output.quantity < Decimal::ZERO {
            return Err(invalid(format!(
                "output `{}` quantity was negative, was {}",
                output.instrument_id, output.quantity
            )));
        }
    }
    let mut receipts: Vec<_> = outputs
        .iter()
        .filter(|output| !output.quantity.is_zero())
        .copied()
        .collect();
    receipts.sort_unstable_by_key(|receipt| receipt.instrument_id);
    check_duplicate_instruments(
        receipts
            .windows(2)
            .map(|pair| (pair[0].instrument_id, pair[1].instrument_id)),
        "output",
    )?;

    let net_cash_units = amount_to_units(net_cash, precision, "net cash")?;

    let mut total_cost_units = U512::ZERO;
    for (_, _, _, cost_units) in &legs {
        total_cost_units = total_cost_units
            .checked_add(*cost_units)
            .ok_or_else(|| invalid("total released cost cannot be represented"))?;
    }

    if !precise {
        units_to_decimal(total_cost_units, precision)?;
    }

    let has_outputs = !receipts.is_empty();
    let (carryover_units, pnl_units, pnl_negative) = if has_outputs {
        if total_cost_units >= net_cash_units {
            (total_cost_units - net_cash_units, U512::ZERO, false)
        } else {
            (U512::ZERO, net_cash_units - total_cost_units, false)
        }
    } else if net_cash_units >= total_cost_units {
        (U512::ZERO, net_cash_units - total_cost_units, false)
    } else {
        (U512::ZERO, total_cost_units - net_cash_units, true)
    };

    if !precise {
        units_to_decimal(carryover_units, precision)?;
        units_to_decimal(pnl_units, precision)?;
    }

    let pnl_weights = if total_cost_units.is_zero() {
        legs.iter()
            .map(|(_, quantity, _, _)| *quantity)
            .collect::<Vec<_>>()
    } else {
        legs.iter()
            .map(|(_, _, released_cost, _)| *released_cost)
            .collect::<Vec<_>>()
    };
    let pnl_values = allocate_units(pnl_units, pnl_negative, &pnl_weights, precision)?;
    let input_pnl = legs
        .iter()
        .zip(pnl_values)
        .map(|((instrument_id, _, _, _), amount)| (*instrument_id, amount))
        .collect();

    let output_costs = if receipts.is_empty() {
        Vec::new()
    } else {
        let weights = receipts
            .iter()
            .map(|receipt| receipt.quantity)
            .collect::<Vec<_>>();
        let costs = allocate_units(carryover_units, false, &weights, precision)?;
        receipts
            .iter()
            .zip(costs)
            .map(|(receipt, cost)| (receipt.instrument_id, cost))
            .collect()
    };

    Ok(PolymarketConvertAllocation {
        input_pnl,
        output_costs,
    })
}

fn invalid(message: impl Into<String>) -> CorrectnessError {
    CorrectnessError::PredicateViolation {
        message: message.into(),
    }
}

fn amount_to_units(value: Decimal, precision: u32, field: &str) -> CorrectnessResult<U512> {
    if value < Decimal::ZERO {
        return Err(invalid(format!(
            "`{field}` cannot be negative, was {value}"
        )));
    }

    let normalized = value.normalize();
    if normalized.scale() > precision {
        return Err(invalid(format!(
            "`{field}` cannot be represented exactly at settlement precision {precision}, was {value}"
        )));
    }

    let factor = U512::from(10_u8).pow(U512::from(precision - normalized.scale()));
    U512::from(normalized.mantissa().unsigned_abs())
        .checked_mul(factor)
        .ok_or_else(|| {
            invalid(format!(
                "`{field}` cannot be represented as settlement currency units, was {value}"
            ))
        })
}

fn check_duplicate_instruments(
    pairs: impl Iterator<Item = (InstrumentId, InstrumentId)>,
    side: &str,
) -> CorrectnessResult<()> {
    for (lhs, rhs) in pairs {
        if lhs == rhs {
            return Err(invalid(format!("duplicate {side} instrument ID `{lhs}`")));
        }
    }

    Ok(())
}

/// Allocates `total_units` across nonnegative `weights`, rounding each magnitude down and giving
/// the final positive-weight entry the remainder.
fn allocate_units(
    total_units: U512,
    negative: bool,
    weights: &[Decimal],
    precision: u32,
) -> CorrectnessResult<Vec<Decimal>> {
    let weight_scale = weights.iter().map(Decimal::scale).max().unwrap_or(0);
    let ten = U512::from(10_u8);

    let mut aligned_weights = Vec::with_capacity(weights.len());
    let mut total_weight = U512::ZERO;
    for weight in weights {
        let factor = ten.pow(U512::from(weight_scale - weight.scale()));
        let aligned = U512::from(weight.mantissa().unsigned_abs())
            .checked_mul(factor)
            .ok_or_else(|| invalid("allocation weight cannot be represented"))?;
        total_weight = total_weight
            .checked_add(aligned)
            .ok_or_else(|| invalid("total allocation weight cannot be represented"))?;
        aligned_weights.push(aligned);
    }

    let last_positive = aligned_weights
        .iter()
        .rposition(|weight| !weight.is_zero())
        .ok_or_else(|| invalid("allocation weights were all zero"))?;

    let mut allocated = U512::ZERO;
    let mut amounts = Vec::with_capacity(weights.len());

    for (index, weight) in aligned_weights.into_iter().enumerate() {
        let share = if index == last_positive {
            total_units
                .checked_sub(allocated)
                .ok_or_else(|| invalid("allocated amount exceeded the total"))?
        } else if weight.is_zero() {
            U512::ZERO
        } else {
            let numerator = total_units
                .checked_mul(weight)
                .ok_or_else(|| invalid("allocation product cannot be represented"))?;
            let share = numerator / total_weight;
            allocated = allocated
                .checked_add(share)
                .ok_or_else(|| invalid("allocated amount cannot be represented"))?;
            share
        };

        let amount = units_to_decimal(share, precision)?;
        amounts.push(if negative && !share.is_zero() {
            -amount
        } else {
            amount
        });
    }

    Ok(amounts)
}

fn units_to_decimal(mut units: U512, precision: u32) -> CorrectnessResult<Decimal> {
    let ten = U512::from(10_u8);
    let mut scale = precision;
    while scale > 0 && units % ten == U512::ZERO {
        units /= ten;
        scale -= 1;
    }

    i128::try_from(&units)
        .ok()
        .and_then(|raw| from_raw(raw, scale).ok())
        .ok_or_else(|| {
            invalid("amount cannot be represented exactly at settlement currency precision")
        })
}

#[cfg(test)]
mod tests;
