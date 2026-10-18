//! Convert values directly: ordinary objects that resemble Extended JSON remain documents.
use ::bson::{oid::ObjectId, Binary, Bson, DateTime, Document};
use base64::{engine::general_purpose::STANDARD, Engine};
use rquickjs::{
    class::Trace, function::This, Array, BigInt, Class, Coerced, Ctx, Exception, FromJs, Function,
    IntoJs, JsLifetime, Object, Result, Value,
};
use std::str::FromStr;

#[derive(Clone, Trace, JsLifetime)]
#[rquickjs::class]
pub struct Scalar {
    #[qjs(skip_trace)]
    value: Bson,
}

#[rquickjs::methods]
impl Scalar {
    #[qjs(rename = "toJSON")]
    fn to_json<'js>(&self, ctx: Ctx<'js>) -> Result<Value<'js>> {
        ctx.json_parse(self.value.clone().into_canonical_extjson().to_string())
    }

    #[qjs(rename = "toString")]
    fn string(&self) -> String {
        match &self.value {
            Bson::ObjectId(id) => id.to_hex(),
            Bson::Int64(n) => n.to_string(),
            Bson::Double(n) => n.to_string(),
            Bson::Decimal128(n) => n.to_string(),
            other => other.clone().into_canonical_extjson().to_string(),
        }
    }

    #[qjs(rename = "valueOf")]
    fn primitive<'js>(&self, ctx: Ctx<'js>) -> Result<Value<'js>> {
        match self.value {
            Bson::Int64(n) => Ok(BigInt::from_i64(ctx, n)?.into_value()),
            Bson::Double(n) => n.into_js(&ctx),
            _ => self.string().into_js(&ctx),
        }
    }
}

pub fn error(ctx: &Ctx<'_>, message: impl std::fmt::Display) -> rquickjs::Error {
    Exception::throw_type(ctx, &message.to_string())
}

pub fn scalar<'js>(ctx: Ctx<'js>, kind: String, text: String, subtype: u8) -> Result<Value<'js>> {
    let value = match kind.as_str() {
        "ObjectId" => Bson::ObjectId(if text.is_empty() {
            ObjectId::new()
        } else {
            ObjectId::parse_str(&text).map_err(|e| error(&ctx, e))?
        }),
        "NumberInt" => Bson::Int32(text.parse().map_err(|e| error(&ctx, e))?),
        "NumberLong" => Bson::Int64(text.parse().map_err(|e| error(&ctx, e))?),
        "NumberDouble" => Bson::Double(text.parse().map_err(|e| error(&ctx, e))?),
        "NumberDecimal" => {
            Bson::Decimal128(::bson::Decimal128::from_str(&text).map_err(|e| error(&ctx, e))?)
        }
        "BinData" => Bson::Binary(Binary {
            subtype: subtype.into(),
            bytes: STANDARD.decode(text).map_err(|e| error(&ctx, e))?,
        }),
        _ => return Err(error(&ctx, "unknown BSON constructor")),
    };
    to_js(&ctx, value)
}

pub fn from_js<'js>(ctx: &Ctx<'js>, value: Value<'js>) -> Result<Bson> {
    convert(ctx, value, 0, &mut (16 * 1024 * 1024))
}

fn convert<'js>(
    ctx: &Ctx<'js>,
    value: Value<'js>,
    depth: usize,
    budget: &mut usize,
) -> Result<Bson> {
    charge(ctx, budget, 16)?;
    if depth > 64 {
        return Err(error(
            ctx,
            "BSON nesting exceeds 64 levels (or a cyclic object)",
        ));
    }
    if value.is_null() {
        return Ok(Bson::Null);
    }
    if let Some(v) = value.as_bool() {
        return Ok(Bson::Boolean(v));
    }
    if let Some(v) = value.as_string() {
        let text = v.to_string()?;
        charge(ctx, budget, text.len())?;
        return Ok(Bson::String(text));
    }
    if value.is_big_int() {
        let Coerced(text) = Coerced::<String>::from_js(value.ctx(), value.clone())?;
        return Ok(Bson::Int64(
            text.parse()
                .map_err(|_| error(ctx, "BigInt is outside BSON Int64 range"))?,
        ));
    }
    if let Some(n) = value.as_number() {
        if n.is_finite() && n.fract() == 0.0 && n.abs() > 9_007_199_254_740_991.0 {
            return Err(error(
                ctx,
                "unsafe integer: use NumberLong('...'), BigInt, or NumberDouble(...)",
            ));
        }
        return Ok(
            if n >= i32::MIN as f64
                && n <= i32::MAX as f64
                && n.fract() == 0.0
                && !(n == 0.0 && n.is_sign_negative())
            {
                Bson::Int32(n as i32)
            } else {
                Bson::Double(n)
            },
        );
    }
    if value.is_function() || value.is_undefined() || value.is_symbol() {
        return Err(error(
            ctx,
            "functions, undefined, and symbols are not BSON values",
        ));
    }
    let object = value
        .as_object()
        .ok_or_else(|| error(ctx, "unsupported BSON value"))?;
    if let Ok(scalar) = object.clone().into_class::<Scalar>() {
        let scalar = scalar.borrow();
        let bytes =
            ::bson::to_vec(&::bson::doc! {"v": &scalar.value}).map_err(|e| error(ctx, e))?;
        charge(ctx, budget, bytes.len())?;
        return Ok(scalar.value.clone());
    }
    if let Some(array) = value.as_array() {
        return array
            .iter::<Value>()
            .map(|v| convert(ctx, v?, depth + 1, budget))
            .collect::<Result<Vec<_>>>()
            .map(Bson::Array);
    }
    let date: Function = ctx.globals().get("Date")?;
    if object.is_instance_of(date) {
        let get_time: Function = ctx.eval("Date.prototype.getTime")?;
        let ms: f64 = get_time.call((This(object.clone()),))?;
        if !ms.is_finite() {
            return Err(error(ctx, "invalid Date"));
        }
        return Ok(Bson::DateTime(DateTime::from_millis(ms as i64)));
    }
    let regexp: Function = ctx.globals().get("RegExp")?;
    if object.is_instance_of(regexp) {
        let pattern: String = object.get("source")?;
        let options: String = object.get("flags")?;
        charge(ctx, budget, pattern.len() + options.len())?;
        return Ok(Bson::RegularExpression(::bson::Regex { pattern, options }));
    }
    let plain: Object = ctx.eval("Object.prototype")?;
    if object.get_prototype().is_some_and(|p| p != plain) {
        return Err(error(
            ctx,
            "only plain objects, arrays, Dates, RegExps, and BSON values can be stored",
        ));
    }
    let symbols: Function = ctx.eval("Object.getOwnPropertySymbols")?;
    let symbols: Array = symbols.call((object.clone(),))?;
    if !symbols.is_empty() {
        return Err(error(ctx, "symbol keys cannot be stored in BSON"));
    }
    let mut doc = Document::new();
    for entry in object.props::<String, Value>() {
        let (key, value) = entry?;
        charge(ctx, budget, key.len())?;
        doc.insert(key, convert(ctx, value, depth + 1, budget)?);
    }
    Ok(Bson::Document(doc))
}

pub fn to_js<'js>(ctx: &Ctx<'js>, value: Bson) -> Result<Value<'js>> {
    match value {
        Bson::Null => Ok(Value::new_null(ctx.clone())),
        Bson::Boolean(v) => v.into_js(ctx),
        Bson::String(v) => v.into_js(ctx),
        Bson::Int32(v) => v.into_js(ctx),
        Bson::Document(doc) => {
            let object = Object::new(ctx.clone())?;
            for (key, value) in doc {
                // Define an own property so a stored __proto__ key cannot change the prototype.
                object.prop(
                    key,
                    rquickjs::object::Property::from(to_js(ctx, value)?)
                        .writable()
                        .enumerable()
                        .configurable(),
                )?;
            }
            Ok(object.into_value())
        }
        Bson::Array(values) => {
            let array = Array::new(ctx.clone())?;
            for (index, value) in values.into_iter().enumerate() {
                array.set(index, to_js(ctx, value)?)?;
            }
            Ok(array.into_value())
        }
        Bson::DateTime(date) if date.timestamp_millis().unsigned_abs() <= 8_640_000_000_000_000 => {
            let constructor: rquickjs::function::Constructor = ctx.globals().get("Date")?;
            constructor.construct::<_, Value>((date.timestamp_millis() as f64,))
        }
        value => Ok(Class::instance(ctx.clone(), Scalar { value })?.into_value()),
    }
}

/// The native command boundary accepts and returns documents, never JSON text.
pub struct CommandDocument(pub Document);
impl<'js> FromJs<'js> for CommandDocument {
    fn from_js(ctx: &Ctx<'js>, value: Value<'js>) -> Result<Self> {
        match from_js(ctx, value)? {
            Bson::Document(doc) => Ok(Self(doc)),
            _ => Err(error(ctx, "command must be a document")),
        }
    }
}
impl<'js> IntoJs<'js> for CommandDocument {
    fn into_js(self, ctx: &Ctx<'js>) -> Result<Value<'js>> {
        to_js(ctx, Bson::Document(self.0))
    }
}

pub fn size<'js>(ctx: Ctx<'js>, value: Value<'js>) -> Result<usize> {
    let value = from_js(&ctx, value)?;
    ::bson::to_vec(&::bson::doc! {"value": value})
        .map(|bytes| bytes.len())
        .map_err(|e| error(&ctx, e))
}

fn charge(ctx: &Ctx<'_>, budget: &mut usize, bytes: usize) -> Result<()> {
    *budget = budget
        .checked_sub(bytes)
        .ok_or_else(|| error(ctx, "BSON conversion exceeds the 16 MiB shell budget"))?;
    Ok(())
}
