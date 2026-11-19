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

#![warn(clippy::clone_on_ref_ptr)]

mod test_market_data_sbe;

#[cfg(feature = "arrow")]
mod arrow_account_id;
#[cfg(feature = "arrow")]
mod arrow_custom_data_macro;

#[cfg(feature = "arrow")]
mod position_convert {
    use std::slice;

    use arrow::record_batch::RecordBatch;
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        events::{
            PositionOpened,
            order::spec::OrderFilledSpec,
            position::{PositionEvent, snapshot::PositionSnapshot},
        },
        identifiers::{AccountId, PositionId, StrategyId, TraderId},
        instruments::{Instrument, InstrumentAny, stubs::binary_option},
        position::{Position, PositionConvert, PositionConvertKind},
        types::{Price, Quantity},
    };
    use nautilus_serialization::arrow::{DecodeTypedFromRecordBatch, EncodeToRecordBatch};
    use rstest::{fixture, rstest};
    use rust_decimal_macros::dec;

    #[fixture]
    fn position() -> (Position, PositionConvert) {
        let instrument = InstrumentAny::BinaryOption(binary_option());
        let operation_id = UUID4::new();
        let change = PositionConvert {
            operation_id,
            trader_id: TraderId::from("TRADER-001"),
            strategy_id: StrategyId::from("STRATEGY-001"),
            instrument_id: instrument.id(),
            position_id: PositionId::from("P-BINARY"),
            account_id: AccountId::from("POLYMARKET-001"),
            cycle_id: operation_id,
            kind: PositionConvertKind::Open {
                previous_cycle_id: None,
            },
            quantity: Quantity::from("100.00"),
            cost: dec!(50),
            realized_pnl: dec!(0),
            ts_event: UnixNanos::from(1),
            ts_init: UnixNanos::from(2),
        };
        (
            Position::new_from_convert(&instrument, change).unwrap(),
            change,
        )
    }

    fn legacy_batch(batch: &RecordBatch) -> RecordBatch {
        let columns = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                (!matches!(field.name().as_str(), "opening_operation_id" | "convert"))
                    .then_some(index)
            })
            .collect::<Vec<_>>();
        batch.project(&columns).unwrap()
    }

    #[rstest]
    fn test_convert_position_arrow_round_trip(position: (Position, PositionConvert)) {
        let (position, change) = position;
        let PositionEvent::PositionOpened(event) = PositionEvent::from_convert(&position, change)
        else {
            panic!("expected a position opening notification");
        };
        let snapshot = PositionSnapshot::from_replay_state(&position, None);
        let batch =
            PositionOpened::encode_batch(&Default::default(), slice::from_ref(&event)).unwrap();
        let snapshot_batch =
            PositionSnapshot::encode_batch(&Default::default(), slice::from_ref(&snapshot))
                .unwrap();
        let decoded_snapshot = PositionSnapshot::decode_typed_batch(
            snapshot_batch.schema().metadata(),
            snapshot_batch,
        )
        .unwrap();

        for field in ["opening_order_id", "last_px"] {
            let index = batch.schema().index_of(field).unwrap();
            assert!(batch.schema().field(index).is_nullable());
            assert!(batch.column(index).is_null(0));
        }

        let decoded = PositionOpened::decode_typed_batch(batch.schema().metadata(), batch).unwrap();
        assert_eq!(decoded, vec![event]);
        assert_eq!(decoded_snapshot, vec![snapshot]);
    }

    #[rstest]
    fn test_convert_schema_reads_legacy_clob_batches() {
        let instrument = InstrumentAny::BinaryOption(binary_option());
        let fill = OrderFilledSpec::builder()
            .instrument_id(instrument.id())
            .currency(instrument.quote_currency())
            .position_id(PositionId::from("P-BINARY"))
            .last_qty(Quantity::from("10.00"))
            .last_px(Price::from("0.400"))
            .build();
        let position = Position::new(&instrument, fill.clone());
        let event = PositionOpened::create(&position, &fill, UUID4::new(), UnixNanos::from(2));
        let snapshot = PositionSnapshot::from(&position, None);
        let batch = legacy_batch(
            &PositionOpened::encode_batch(&Default::default(), slice::from_ref(&event)).unwrap(),
        );
        let snapshot_batch = legacy_batch(
            &PositionSnapshot::encode_batch(&Default::default(), slice::from_ref(&snapshot))
                .unwrap(),
        );

        assert_eq!(
            PositionOpened::decode_typed_batch(batch.schema().metadata(), batch).unwrap(),
            vec![event]
        );
        assert_eq!(
            PositionSnapshot::decode_typed_batch(
                snapshot_batch.schema().metadata(),
                snapshot_batch
            )
            .unwrap(),
            vec![snapshot]
        );
    }

    #[cfg(feature = "arrow-display")]
    #[rstest]
    fn test_convert_position_display_arrow(position: (Position, PositionConvert)) {
        use nautilus_serialization::arrow::display::position::encode_positions;

        let (position, _) = position;
        let batch = encode_positions(&[position]).unwrap();
        let order_column = batch.schema().index_of("opening_order_id").unwrap();

        assert!(batch.schema().field(order_column).is_nullable());
        assert!(batch.column(order_column).is_null(0));
    }
}
