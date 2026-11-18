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

use proptest::prelude::*;
use rstest::rstest;
use rust_decimal_macros::dec;

use super::*;
use crate::types::fixed::FIXED_PRECISION;

const NO_A: &str = "NO-A.POLYMARKET";
const NO_B: &str = "NO-B.POLYMARKET";
const NO_C: &str = "NO-C.POLYMARKET";
const YES_A: &str = "YES-A.POLYMARKET";
const YES_B: &str = "YES-B.POLYMARKET";
const YES_C: &str = "YES-C.POLYMARKET";

fn input(id: &str, quantity: Decimal, released_cost: Decimal) -> PolymarketConvertInput {
    PolymarketConvertInput {
        instrument_id: InstrumentId::from(id),
        quantity,
        released_cost,
    }
}

fn output(id: &str, quantity: Decimal) -> PolymarketConvertOutput {
    PolymarketConvertOutput {
        instrument_id: InstrumentId::from(id),
        quantity,
    }
}

fn allocation(id: &str, amount: Decimal) -> (InstrumentId, Decimal) {
    (InstrumentId::from(id), amount)
}

fn assert_conservation(
    total_cost: Decimal,
    net_cash: Decimal,
    result: &PolymarketConvertAllocation,
) {
    let pnl: Decimal = result.input_pnl.iter().map(|(_, amount)| amount).sum();
    let carryover: Decimal = result.output_costs.iter().map(|(_, amount)| amount).sum();
    assert_eq!(total_cost + pnl, net_cash + carryover);
}

#[rstest]
#[case(dec!(100), dec!(50), dec!(0), dec!(0))]
#[case(dec!(99.5), dec!(50.5), dec!(0), dec!(0))]
#[case(dec!(150), dec!(0), dec!(0), dec!(0))]
#[case(dec!(165), dec!(0), dec!(8), dec!(7))]
fn test_cash_and_yes_effects(
    #[case] net_cash: Decimal,
    #[case] carryover: Decimal,
    #[case] pnl_a: Decimal,
    #[case] pnl_b: Decimal,
) {
    let inputs = [
        input(NO_B, dec!(100), dec!(70)),
        input(NO_A, dec!(100), dec!(80)),
    ];
    let outputs = [output(YES_C, dec!(100))];
    let result = allocate_convert_costs(&inputs, &outputs, net_cash, Currency::pUSD()).unwrap();

    assert_eq!(result.output_costs, [allocation(YES_C, carryover)]);
    assert_eq!(
        result.input_pnl,
        [allocation(NO_A, pnl_a), allocation(NO_B, pnl_b)]
    );
    assert_conservation(dec!(150), net_cash, &result);
}

#[rstest]
#[case(dec!(4), dec!(-0.4), dec!(-0.6))]
#[case(dec!(4.999999), dec!(0), dec!(-0.000001))]
#[case(dec!(5), dec!(0), dec!(0))]
#[case(dec!(7), dec!(0.8), dec!(1.2))]
fn test_cash_only_effects(
    #[case] net_cash: Decimal,
    #[case] pnl_a: Decimal,
    #[case] pnl_b: Decimal,
) {
    let inputs = [
        input(NO_C, dec!(8), dec!(0)),
        input(NO_A, dec!(1), dec!(2)),
        input(NO_B, dec!(2), dec!(3)),
    ];
    let outputs = [output(YES_A, dec!(0))];
    let result = allocate_convert_costs(&inputs, &outputs, net_cash, Currency::pUSD()).unwrap();

    assert!(result.output_costs.is_empty());
    assert_eq!(
        result.input_pnl,
        [
            allocation(NO_A, pnl_a),
            allocation(NO_B, pnl_b),
            allocation(NO_C, dec!(0))
        ],
    );
    assert_conservation(dec!(5), net_cash, &result);
}

#[rstest]
fn test_quantity_weighted_carryover_is_order_independent() {
    let mut inputs = [
        input(NO_B, dec!(1), dec!(4)),
        input(NO_A, dec!(2), dec!(5.000001)),
    ];
    let mut outputs = [
        output(YES_C, dec!(1.5)),
        output(YES_A, dec!(0.5)),
        output(YES_B, dec!(1)),
    ];
    let result = allocate_convert_costs(&inputs, &outputs, dec!(8), Currency::pUSD()).unwrap();
    inputs.reverse();
    outputs.rotate_left(1);
    let reordered = allocate_convert_costs(&inputs, &outputs, dec!(8), Currency::pUSD()).unwrap();

    assert_eq!(result, reordered);
    assert_eq!(
        result.output_costs,
        [
            allocation(YES_A, dec!(0.166666)),
            allocation(YES_B, dec!(0.333333)),
            allocation(YES_C, dec!(0.500002)),
        ],
    );
    assert_eq!(
        result.input_pnl,
        [allocation(NO_A, dec!(0)), allocation(NO_B, dec!(0))]
    );
    assert_conservation(dec!(9.000001), dec!(8), &result);
}

#[rstest]
fn test_zero_cost_pnl_uses_consumed_quantity() {
    let mut inputs = [input(NO_B, dec!(2), dec!(0)), input(NO_A, dec!(1), dec!(0))];
    let outputs = [output(YES_A, dec!(1))];
    let net_cash = dec!(1.000001);
    let result = allocate_convert_costs(&inputs, &outputs, net_cash, Currency::pUSD()).unwrap();
    inputs.reverse();
    let reordered = allocate_convert_costs(&inputs, &outputs, net_cash, Currency::pUSD()).unwrap();

    assert_eq!(result, reordered);
    assert_eq!(result.output_costs, [allocation(YES_A, dec!(0))]);
    assert_eq!(
        result.input_pnl,
        [
            allocation(NO_A, dec!(0.333333)),
            allocation(NO_B, dec!(0.666668))
        ],
    );
    assert_conservation(dec!(0), net_cash, &result);
}

#[rstest]
fn test_share_just_under_one_settlement_unit() {
    let currency = Currency {
        precision: FIXED_PRECISION,
        ..Currency::pUSD()
    };
    let unit = Decimal::new(1, u32::from(currency.precision));
    let inputs = [input(NO_A, dec!(1), unit)];
    let outputs = [
        output(YES_A, Decimal::MAX - Decimal::ONE),
        output(YES_B, Decimal::ONE),
        output(YES_C, Decimal::ZERO),
    ];
    let result = allocate_convert_costs(&inputs, &outputs, Decimal::ZERO, currency).unwrap();

    assert_eq!(
        result.output_costs,
        [allocation(YES_A, Decimal::ZERO), allocation(YES_B, unit)]
    );
    assert_conservation(unit, Decimal::ZERO, &result);
}

#[rstest]
#[case(dec!(0), dec!(1), dec!(1), dec!(0))]
#[case(dec!(-1), dec!(1), dec!(1), dec!(0))]
#[case(dec!(1), dec!(-1), dec!(1), dec!(0))]
#[case(dec!(1), dec!(1), dec!(-1), dec!(0))]
#[case(dec!(1), dec!(0.0000001), dec!(1), dec!(0))]
#[case(dec!(1), dec!(1), dec!(1), dec!(0.0000001))]
fn test_invalid_input_values(
    #[case] consumed: Decimal,
    #[case] released_cost: Decimal,
    #[case] received: Decimal,
    #[case] net_cash: Decimal,
) {
    let inputs = [input(NO_A, consumed, released_cost)];
    let outputs = [output(YES_A, received)];
    let result = allocate_convert_costs(&inputs, &outputs, net_cash, Currency::pUSD());

    assert!(result.is_err());
    assert_eq!(inputs, [input(NO_A, consumed, released_cost)]);
    assert_eq!(outputs, [output(YES_A, received)]);
}

#[rstest]
fn test_negative_net_cash_error() {
    let inputs = [input(NO_A, dec!(1), dec!(1))];
    let error =
        allocate_convert_costs(&inputs, &[], dec!(-0.000001), Currency::pUSD()).unwrap_err();

    assert!(error.to_string().contains("cash"));
}

#[rstest]
fn test_missing_or_duplicate_instruments() {
    let leg = input(NO_A, dec!(1), dec!(1));
    let receipt = output(YES_A, dec!(1));
    let currency = Currency::pUSD();

    assert!(allocate_convert_costs(&[], &[receipt], dec!(0), currency).is_err());
    assert!(allocate_convert_costs(&[leg, leg], &[receipt], dec!(0), currency).is_err());
    assert!(allocate_convert_costs(&[leg], &[receipt, receipt], dec!(0), currency).is_err());
}

#[rstest]
fn test_unrepresentable_amounts() {
    let inputs = [
        input(NO_A, dec!(1), Decimal::MAX),
        input(NO_B, dec!(1), Decimal::ONE),
    ];
    let outputs = [output(YES_A, dec!(1)), output(YES_B, dec!(1))];
    let currency = Currency::pUSD();

    assert!(allocate_convert_costs(&inputs, &[], dec!(0), currency).is_err());
    assert!(allocate_convert_costs(&inputs[..1], &outputs, dec!(0), currency).is_err());
}

#[rstest]
fn test_total_cost_does_not_round_intermediate_sums() {
    let inputs = [
        input(NO_A, dec!(1), Decimal::MAX - Decimal::ONE),
        input(NO_B, dec!(1), dec!(0.4)),
        input(NO_C, dec!(1), dec!(0.6)),
    ];
    let result = allocate_convert_costs(&inputs, &[], Decimal::MAX, Currency::pUSD()).unwrap();

    assert!(result.output_costs.is_empty());
    assert_eq!(
        result.input_pnl,
        [
            allocation(NO_A, Decimal::ZERO),
            allocation(NO_B, Decimal::ZERO),
            allocation(NO_C, Decimal::ZERO),
        ],
    );
    assert_conservation(Decimal::MAX, Decimal::MAX, &result);
}

proptest! {
    #[rstest]
    fn test_exact_allocation_under_permutation(
        costs in (0..1000_i64, 0..1000_i64, 0..1000_i64),
        quantities in (1..1000_i64, 1..1000_i64, 1..1000_i64),
        received in (0..1000_i64, 0..1000_i64, 0..1000_i64),
        net_cash_units in 0..3000_i64,
        precision in 0..=FIXED_PRECISION,
    ) {
        let currency = Currency { precision, ..Currency::pUSD() };
        let money = |units| Decimal::new(units, u32::from(precision));
        let units = |quantity| Decimal::new(quantity, 3);
        let mut inputs = [
            input(NO_A, units(quantities.0), money(costs.0)),
            input(NO_B, units(quantities.1), money(costs.1)),
            input(NO_C, units(quantities.2), money(costs.2)),
        ];
        let mut outputs = [
            output(YES_A, units(received.0)),
            output(YES_B, units(received.1)),
            output(YES_C, units(received.2)),
        ];
        let total_cost = money(costs.0 + costs.1 + costs.2);
        let net_cash = money(net_cash_units);
        let result = allocate_convert_costs(&inputs, &outputs, net_cash, currency).unwrap();
        inputs.rotate_left(1);
        outputs.reverse();
        let reordered = allocate_convert_costs(&inputs, &outputs, net_cash, currency).unwrap();

        prop_assert_eq!(&result, &reordered);
        assert_conservation(total_cost, net_cash, &result);
        prop_assert!(result.output_costs.iter().all(|(_, amount)| *amount >= Decimal::ZERO));
        prop_assert!(result.input_pnl.iter().chain(&result.output_costs).all(
            |(_, amount)| amount.normalize().scale() <= u32::from(precision),
        ));
    }
}
