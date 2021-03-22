use serde::de;
use serde::ser::{self, Impossible, Serialize};
use std::collections::HashMap;
use std::fmt::{self, Display};

pub type Result<T> = std::result::Result<T, Error>;

// This is a bare-bones implementation. A real library would provide additional
// information in its error type, for example the line and column at which the
// error occurred, the byte offset into the input, or the current key being
// processed.
#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    Message(String),
    UnsupportedType,
}

impl ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Message(msg) => write!(f, "{}", msg),
            Error::UnsupportedType => f.write_str("unsupported type"),
        }
    }
}

impl std::error::Error for Error {}

pub struct Serializer {
    output: HashMap<String, String>,
    value: Option<String>,
}

pub fn to_hash_map<T>(value: &T) -> Result<HashMap<String, String>>
where
    T: Serialize,
{
    let mut serializer = Serializer {
        output: HashMap::new(),
        value: None,
    };
    value.serialize(&mut serializer)?;
    Ok(serializer.output)
}

impl<'a> ser::Serializer for &'a mut Serializer {
    // The output type produced by this `Serializer` during successful
    // serialization. Most serializers that produce text or binary output should
    // set `Ok = ()` and serialize into an `io::Write` or buffer contained
    // within the `Serializer` instance, as happens here. Serializers that build
    // in-memory data structures may be simplified by using `Ok` to propagate
    // the data structure around.
    type Ok = ();

    type Error = Error;

    type SerializeSeq = Impossible<(), Error>;
    type SerializeTuple = Impossible<(), Error>;
    type SerializeTupleStruct = Impossible<(), Error>;
    type SerializeTupleVariant = Impossible<(), Error>;
    type SerializeMap = Impossible<(), Error>;
    type SerializeStruct = Self;
    type SerializeStructVariant = Impossible<(), Error>;

    fn serialize_bool(self, v: bool) -> Result<()> {
        let value = if v {
            "true".to_string()
        } else {
            "false".to_string()
        };

        self.value = Some(value);
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.serialize_i64(i64::from(v))
    }

    // Not particularly efficient but this is example code anyway. A more
    // performant approach would be to use the `itoa` crate.
    fn serialize_i64(self, v: i64) -> Result<()> {
        self.value = Some(v.to_string());
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.value = Some(v.to_string());
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        self.serialize_f64(f64::from(v))
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        self.value = Some(v.to_string());
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<()> {
        self.value = Some(v.to_string());
        Ok(())
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        self.value = Some(v.to_string());
        Ok(())
    }

    // Serialize a byte array as an array of bytes. Could also use a base64
    // string here. Binary formats will typically represent byte arrays more
    // compactly.
    fn serialize_bytes(self, _: &[u8]) -> Result<()> {
        println!("bytes");
        Err(Error::UnsupportedType)
    }

    // An absent optional is represented as the JSON `null`.
    fn serialize_none(self) -> Result<()> {
        self.value = None;
        Ok(())
    }

    // A present optional is represented as just the contained value. Note that
    // this is a lossy representation. For example the values `Some(())` and
    // `None` both serialize as just `null`. Unfortunately this is typically
    // what people expect when working with JSON. Other formats are encouraged
    // to behave more intelligently if possible.
    fn serialize_some<T>(self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<()> {
        self.value = None;
        Ok(())
    }

    // Unit struct means a named value containing no data. Again, since there is
    // no data, map this to JSON as `null`. There is no need to serialize the
    // name in most formats.
    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        self.serialize_unit()
    }

    // When serializing a unit variant (or any other kind of variant), formats
    // can choose whether to keep track of it by index or by name. Binary
    // formats typically use the index of the variant and human-readable formats
    // typically use the name.
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<()> {
        self.serialize_str(variant)
    }

    // As is done here, serializers are encouraged to treat newtype structs as
    // insignificant wrappers around the data they contain.
    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    // Note that newtype variant (and all of the other variant serialization
    // methods) refer exclusively to the "externally tagged" enum
    // representation.
    //
    // Serialize this to JSON in externally tagged form as `{ NAME: VALUE }`.
    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _: &'static str,
        _: &T,
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        println!("newtype variant");
        Err(Error::UnsupportedType)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        println!("seq");
        Err(Error::UnsupportedType)
    }

    // Tuples look just like sequences in JSON. Some formats may be able to
    // represent tuples more efficiently by omitting the length, since tuple
    // means that the corresponding `Deserialize implementation will know the
    // length without needing to look at the serialized data.
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.serialize_seq(Some(len))
    }

    // Tuple structs look just like sequences in JSON.
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_seq(Some(len))
    }

    // Tuple variants are represented in JSON as `{ NAME: [DATA...] }`. Again
    // this method is only responsible for the externally tagged representation.
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        variant.serialize(&mut *self)?;
        println!("tuple variant");
        Err(Error::UnsupportedType)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        println!("map");
        Err(Error::UnsupportedType)
    }

    fn serialize_struct(self, _name: &'static str, _: usize) -> Result<Self::SerializeStruct> {
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        variant.serialize(&mut *self)?;
        println!("struct variant");
        Err(Error::UnsupportedType)
    }
}

impl<'a> ser::SerializeMap for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T>(&mut self, _: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        println!("map key");
        Err(Error::UnsupportedType)
    }

    fn serialize_value<T>(&mut self, _: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        println!("map value");
        Err(Error::UnsupportedType)
    }

    fn serialize_entry<K: ?Sized, V: ?Sized>(&mut self, key: &K, value: &V) -> Result<()>
    where
        K: Serialize,
        V: Serialize,
    {
        key.serialize(&mut **self)?;
        let key = self.value.take();

        value.serialize(&mut **self)?;
        let value = self.value.take();

        if let (Some(key), Some(value)) = (key, value) {
            self.output.insert(key, value);
        }

        Ok(())
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

// Structs are like maps in which the keys are constrained to be compile-time
// constant strings.
impl<'a> ser::SerializeStruct for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)?;
        let value = self.value.take();
        if let Some(value) = value {
            self.output.insert(key.to_string(), value);
        }
        Ok(())
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::to_hash_map;
    use serde::Serialize;

    #[test]
    fn test_struct() {
        #[derive(Serialize)]
        struct Test {
            int: u32,
            string: String,
            opt_none_str: Option<String>,
            opt_some_str: Option<String>,
        }

        let test = Test {
            int: 1,
            string: "test_string".to_string(),
            opt_none_str: None,
            opt_some_str: Some("test_opt_str".to_string()),
        };

        let foo = to_hash_map(&test).unwrap();
        println!("{:?}", foo);
    }
}
