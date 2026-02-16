use rust_decimal::prelude::ToPrimitive;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;
use openferric::core::{BarrierDirection, BarrierStyle, ExerciseStyle, OptionType, PricingEngine};
use openferric::engines::analytic::HestonEngine;
use openferric::instruments::vanilla::VanillaOption;
use openferric::market::Market;
use openferric::pricing::american::crr_binomial_american;
use openferric::pricing::barrier::barrier_price_closed_form;
use openferric::pricing::european::{black_scholes_greeks, black_scholes_price};

pub(crate) fn eval_bs_price(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let spot = parse_f64(&args[0], "bs_price() expects numeric spot")?;
    let strike = parse_f64(&args[1], "bs_price() expects numeric strike")?;
    let expiry = parse_f64(&args[2], "bs_price() expects numeric expiry")?;
    let vol = parse_f64(&args[3], "bs_price() expects numeric vol")?;
    let rate = parse_f64(&args[4], "bs_price() expects numeric rate")?;
    let option_type = parse_option_type(&args[5], "bs_price()")?;

    Ok(ScalarValue::Float(black_scholes_price(
        option_type,
        spot,
        strike,
        rate,
        vol,
        expiry,
    )))
}

pub(crate) fn eval_bs_greeks(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let spot = parse_f64(&args[0], "bs_greeks() expects numeric spot")?;
    let strike = parse_f64(&args[1], "bs_greeks() expects numeric strike")?;
    let expiry = parse_f64(&args[2], "bs_greeks() expects numeric expiry")?;
    let vol = parse_f64(&args[3], "bs_greeks() expects numeric vol")?;
    let rate = parse_f64(&args[4], "bs_greeks() expects numeric rate")?;
    let option_type = parse_option_type(&args[5], "bs_greeks()")?;

    let greeks = black_scholes_greeks(option_type, spot, strike, rate, vol, expiry);

    let mut payload = JsonMap::new();
    payload.insert(
        "delta".to_string(),
        finite_json_number(greeks.delta, "bs_greeks() delta")?,
    );
    payload.insert(
        "gamma".to_string(),
        finite_json_number(greeks.gamma, "bs_greeks() gamma")?,
    );
    payload.insert(
        "vega".to_string(),
        finite_json_number(greeks.vega, "bs_greeks() vega")?,
    );
    payload.insert(
        "theta".to_string(),
        finite_json_number(greeks.theta, "bs_greeks() theta")?,
    );
    payload.insert(
        "rho".to_string(),
        finite_json_number(greeks.rho, "bs_greeks() rho")?,
    );

    Ok(ScalarValue::Text(JsonValue::Object(payload).to_string()))
}

pub(crate) fn eval_barrier_price(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let spot = parse_f64(&args[0], "barrier_price() expects numeric spot")?;
    let strike = parse_f64(&args[1], "barrier_price() expects numeric strike")?;
    let expiry = parse_f64(&args[2], "barrier_price() expects numeric expiry")?;
    let vol = parse_f64(&args[3], "barrier_price() expects numeric vol")?;
    let rate = parse_f64(&args[4], "barrier_price() expects numeric rate")?;
    let barrier = parse_f64(&args[5], "barrier_price() expects numeric barrier")?;
    let option_type = parse_option_type(&args[6], "barrier_price()")?;
    let barrier_style = parse_barrier_style(&args[7])?;
    let barrier_direction = parse_barrier_direction(&args[8])?;

    Ok(ScalarValue::Float(barrier_price_closed_form(
        option_type,
        barrier_style,
        barrier_direction,
        spot,
        strike,
        barrier,
        rate,
        vol,
        expiry,
    )))
}

pub(crate) fn eval_american_price(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let spot = parse_f64(&args[0], "american_price() expects numeric spot")?;
    let strike = parse_f64(&args[1], "american_price() expects numeric strike")?;
    let expiry = parse_f64(&args[2], "american_price() expects numeric expiry")?;
    let vol = parse_f64(&args[3], "american_price() expects numeric vol")?;
    let rate = parse_f64(&args[4], "american_price() expects numeric rate")?;
    let option_type = parse_option_type(&args[5], "american_price()")?;
    let steps = if args.len() == 7 {
        parse_positive_steps(&args[6])?
    } else {
        500
    };

    Ok(ScalarValue::Float(crr_binomial_american(
        option_type,
        spot,
        strike,
        rate,
        vol,
        expiry,
        steps,
    )))
}

pub(crate) fn eval_heston_price(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let spot = parse_f64(&args[0], "heston_price() expects numeric spot")?;
    let strike = parse_f64(&args[1], "heston_price() expects numeric strike")?;
    let expiry = parse_f64(&args[2], "heston_price() expects numeric expiry")?;
    let rate = parse_f64(&args[3], "heston_price() expects numeric rate")?;
    let v0 = parse_f64(&args[4], "heston_price() expects numeric v0")?;
    let kappa = parse_f64(&args[5], "heston_price() expects numeric kappa")?;
    let theta = parse_f64(&args[6], "heston_price() expects numeric theta")?;
    let sigma_v = parse_f64(&args[7], "heston_price() expects numeric sigma_v")?;
    let rho = parse_f64(&args[8], "heston_price() expects numeric rho")?;
    let option_type = parse_option_type(&args[9], "heston_price()")?;

    let instrument = VanillaOption {
        option_type,
        strike,
        expiry,
        exercise: ExerciseStyle::European,
    };
    let market = Market::builder()
        .spot(spot)
        .rate(rate)
        .dividend_yield(0.0)
        .flat_vol(1.0)
        .build()
        .map_err(map_pricing_error)?;
    let engine = HestonEngine::new(v0, kappa, theta, sigma_v, rho);
    let result = engine
        .price(&instrument, &market)
        .map_err(map_pricing_error)?;

    Ok(ScalarValue::Float(result.price))
}

fn parse_f64(value: &ScalarValue, message: &str) -> Result<f64, EngineError> {
    match value {
        ScalarValue::Float(v) => Ok(*v),
        ScalarValue::Int(v) => Ok(*v as f64),
        ScalarValue::Numeric(d) => d.to_f64().ok_or_else(|| EngineError {
            message: message.to_string(),
        }),
        ScalarValue::Text(v) => v.parse::<f64>().map_err(|_| EngineError {
            message: message.to_string(),
        }),
        _ => Err(EngineError {
            message: message.to_string(),
        }),
    }
}

fn parse_positive_steps(value: &ScalarValue) -> Result<usize, EngineError> {
    let steps = match value {
        ScalarValue::Int(v) => *v,
        ScalarValue::Float(v) if v.fract() == 0.0 => *v as i64,
        ScalarValue::Text(v) => v.parse::<i64>().map_err(|_| EngineError {
            message: "american_price() steps must be a positive integer".to_string(),
        })?,
        _ => {
            return Err(EngineError {
                message: "american_price() steps must be a positive integer".to_string(),
            });
        }
    };
    if steps <= 0 {
        return Err(EngineError {
            message: "american_price() steps must be a positive integer".to_string(),
        });
    }
    Ok(steps as usize)
}

fn parse_option_type(value: &ScalarValue, context: &str) -> Result<OptionType, EngineError> {
    match normalized_text(value, "option_type must be text")?.as_str() {
        "call" => Ok(OptionType::Call),
        "put" => Ok(OptionType::Put),
        _ => Err(EngineError {
            message: format!("{context} option_type must be 'call' or 'put'"),
        }),
    }
}

fn parse_barrier_style(value: &ScalarValue) -> Result<BarrierStyle, EngineError> {
    match normalized_text(value, "barrier_type must be text")?.as_str() {
        "in" | "knock_in" | "ki" | "knockin" => Ok(BarrierStyle::In),
        "out" | "knock_out" | "ko" | "knockout" => Ok(BarrierStyle::Out),
        _ => Err(EngineError {
            message: "barrier_price() barrier_type must be 'in' or 'out'".to_string(),
        }),
    }
}

fn parse_barrier_direction(value: &ScalarValue) -> Result<BarrierDirection, EngineError> {
    match normalized_text(value, "barrier_dir must be text")?.as_str() {
        "up" => Ok(BarrierDirection::Up),
        "down" => Ok(BarrierDirection::Down),
        _ => Err(EngineError {
            message: "barrier_price() barrier_dir must be 'up' or 'down'".to_string(),
        }),
    }
}

fn normalized_text(value: &ScalarValue, message: &str) -> Result<String, EngineError> {
    match value {
        ScalarValue::Text(text) => Ok(text
            .trim()
            .to_ascii_lowercase()
            .replace('-', "_")
            .replace(' ', "_")),
        _ => Err(EngineError {
            message: message.to_string(),
        }),
    }
}

fn finite_json_number(value: f64, message: &str) -> Result<JsonValue, EngineError> {
    JsonNumber::from_f64(value)
        .map(JsonValue::Number)
        .ok_or_else(|| EngineError {
            message: message.to_string(),
        })
}

fn map_pricing_error(error: openferric::core::PricingError) -> EngineError {
    EngineError {
        message: error.to_string(),
    }
}
