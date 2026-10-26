//! Generate frozen pyclass wrappers from the pinned proto descriptors (E04-D021).
//!
//! Generated Rust is written to OUT_DIR. Generated `.pyi` files are written
//! under `python/mftik/` and are not committed (E01-D005).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use heck::{ToSnakeCase, ToUpperCamelCase};
use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, EnumDescriptorProto, FieldDescriptorProto, FileDescriptorSet};

const LABEL_REPEATED: i32 = Label::Repeated as i32;
const TYPE_MESSAGE: i32 = Type::Message as i32;
const TYPE_ENUM: i32 = Type::Enum as i32;
const TYPE_GROUP: i32 = Type::Group as i32;
const TYPE_BYTES: i32 = Type::Bytes as i32;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let proto_root = manifest_dir.join("../../contract/proto");
    let proto_dir = proto_root.join("mftiko");
    let mut protos = Vec::new();
    collect_protos(&proto_dir, &mut protos)?;
    protos.sort();
    if protos.is_empty() {
        return Err(format!("no .proto files under {}", proto_dir.display()).into());
    }
    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let descriptor = descriptor_set(&proto_root, &protos, &out_dir)?;
    let model = build_model(&descriptor)?;
    let rust = emit_rust(&model);
    fs::write(out_dir.join("proto_gen.rs"), rust)?;
    write_pyi(&manifest_dir.join("../../python/mftik"), &model)?;
    Ok(())
}

fn collect_protos(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_protos(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "proto") {
            out.push(path);
        }
    }
    Ok(())
}

fn descriptor_set(
    proto_root: &Path,
    protos: &[PathBuf],
    out_dir: &Path,
) -> Result<FileDescriptorSet, Box<dyn std::error::Error>> {
    let descriptor_path = out_dir.join("mftik.pb");
    let mut cmd = Command::new(protoc_bin_vendored::protoc_bin_path()?);
    cmd.arg("--descriptor_set_out")
        .arg(&descriptor_path)
        .arg("-I")
        .arg(proto_root);
    for proto in protos {
        cmd.arg(proto);
    }
    let status = cmd.status()?;
    if !status.success() {
        return Err("protoc --descriptor_set_out failed".into());
    }
    let bytes = fs::read(&descriptor_path)?;
    Ok(FileDescriptorSet::decode(bytes.as_slice())?)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scalar {
    F64,
    F32,
    I32,
    I64,
    U32,
    U64,
    Bool,
    String,
}

impl Scalar {
    fn rust(self) -> &'static str {
        match self {
            Self::F64 => "f64",
            Self::F32 => "f32",
            Self::I32 => "i32",
            Self::I64 => "i64",
            Self::U32 => "u32",
            Self::U64 => "u64",
            Self::Bool => "bool",
            Self::String => "String",
        }
    }

    fn py(self) -> &'static str {
        match self {
            Self::F64 | Self::F32 => "float",
            Self::Bool => "bool",
            Self::String => "str",
            _ => "int",
        }
    }
}

struct EnumDef {
    py_module: String,
    name: String,
    values: Vec<(String, i32)>,
}

struct MsgDef {
    py_module: String,
    name: String,
    rust_type: String,
    rust_struct: String,
    slots: Vec<Slot>,
}

enum Slot {
    Plain(FieldDef),
    Branch(BranchDef),
    OneofName(OneofName),
}

struct FieldDef {
    py_name: String,
    rust_field: String,
    kind: Kind,
}

struct BranchDef {
    py_name: String,
    rust_field: String,
    rust_enum: String,
    variant: String,
    kind: Kind,
}

struct OneofName {
    py_name: String,
    rust_field: String,
    rust_enum: String,
    arms: Vec<(String, String)>,
}

enum Kind {
    Scalar {
        scalar: Scalar,
        optional: bool,
    },
    EnumRef {
        py_module: String,
        name: String,
        optional: bool,
    },
    MessageRef {
        rust_struct: String,
        py_module: String,
        name: String,
    },
    Bytes {
        optional: bool,
    },
    Repeated(Box<Kind>),
    Map {
        key: Scalar,
        value: Box<Kind>,
    },
}

struct Model {
    enums: Vec<EnumDef>,
    messages: Vec<MsgDef>,
}

struct Index<'a> {
    messages: &'a BTreeMap<String, &'a DescriptorProto>,
    enums: &'a BTreeMap<String, &'a EnumDescriptorProto>,
    maps: &'a BTreeMap<String, &'a DescriptorProto>,
    packages: &'a BTreeMap<String, String>,
}

fn build_model(set: &FileDescriptorSet) -> Result<Model, Box<dyn std::error::Error>> {
    let mut messages = BTreeMap::new();
    let mut enums = BTreeMap::new();
    let mut maps = BTreeMap::new();
    let mut nested = BTreeSet::new();
    let mut packages = BTreeMap::new();
    for file in &set.file {
        let package = file.package.clone().unwrap_or_default();
        if file.syntax.as_deref().unwrap_or("proto2") != "proto3" {
            return Err(format!("{package} is not proto3").into());
        }
        for message in &file.message_type {
            index_message(
                &package,
                None,
                message,
                &mut messages,
                &mut maps,
                &mut nested,
                &mut packages,
            )?;
        }
        for enum_def in &file.enum_type {
            let name = required_name(enum_def.name.as_deref(), "enum")?;
            let full = format!(".{package}.{name}");
            packages.insert(full.clone(), package.clone());
            enums.insert(full, enum_def);
        }
    }
    let index = Index {
        messages: &messages,
        enums: &enums,
        maps: &maps,
        packages: &packages,
    };

    let mut queue = VecDeque::new();
    for (full, package) in &packages {
        if messages.contains_key(full) && is_seed_package(package) {
            queue.push_back(full.clone());
        }
    }
    for full in [".mftiko.common.v1.TimeInForce", ".mftiko.td.v1.OrderCause"] {
        if !enums.contains_key(full) {
            return Err(format!("missing seeded enum {full}").into());
        }
        queue.push_back(full.to_owned());
    }

    let mut wanted_msgs = BTreeSet::new();
    let mut wanted_enums = BTreeSet::new();
    while let Some(full) = queue.pop_front() {
        if let Some(message) = messages.get(&full) {
            if !wanted_msgs.insert(full.clone()) {
                continue;
            }
            collect_refs(message, &index, &mut queue)?;
        } else if enums.contains_key(&full) {
            wanted_enums.insert(full);
        } else if nested.contains(&full) {
            return Err(
                format!("需要決定: nested message or enum {full} has no Python name").into(),
            );
        } else {
            return Err(format!("unknown proto type {full}").into());
        }
    }

    require_present(&wanted_msgs, &wanted_enums)?;

    let mut enums_out = Vec::new();
    for full in &wanted_enums {
        let enum_def = enums[full];
        let package = &packages[full];
        let name = required_name(enum_def.name.as_deref(), "enum")?;
        let mut values = Vec::new();
        for value in &enum_def.value {
            let value_name = required_name(value.name.as_deref(), "enum value")?;
            if !is_ident(&value_name) {
                return Err(format!("enum value {value_name} is not a Python identifier").into());
            }
            values.push((value_name, value.number.unwrap_or(0)));
        }
        enums_out.push(EnumDef {
            py_module: py_module(package)?,
            name,
            values,
        });
    }

    let mut messages_out = Vec::new();
    for full in &wanted_msgs {
        messages_out.push(message_def(full, &index)?);
    }
    Ok(Model {
        enums: enums_out,
        messages: messages_out,
    })
}

const REQUIRED_MESSAGES: &[&str] = &[
    ".mftiko.md.v1.BestQuote",
    ".mftiko.md.v1.Trade",
    ".mftiko.md.v1.AggTrade",
    ".mftiko.md.v1.Kline",
    ".mftiko.md.v1.Ticker",
    ".mftiko.md.v1.Liquidation",
    ".mftiko.md.v1.FundingRate",
    ".mftiko.md.v1.OpenInterest",
    ".mftiko.md.v1.Greeks",
    ".mftiko.md.v1.MarkPrice",
    ".mftiko.md.v1.OrderBookDelta",
    ".mftiko.md.v1.TapePage",
    ".mftiko.md.v1.BookSnapshot",
    ".mftiko.md.v1.Klines",
    ".mftiko.md.v1.Level",
    ".mftiko.md.v1.Meta",
    ".mftiko.md.v1.FeedEvent",
    ".mftiko.md.v1.TapeEvent",
    ".mftiko.md.v1.Cursor",
    ".mftiko.md.v1.Gap",
    ".mftiko.td.v1.Order",
    ".mftiko.td.v1.Position",
    ".mftiko.td.v1.Balance",
    ".mftiko.td.v1.Fill",
    ".mftiko.td.v1.Recon",
];

const REQUIRED_ENUMS: &[&str] = &[
    ".mftiko.common.v1.Side",
    ".mftiko.common.v1.OrderType",
    ".mftiko.common.v1.TimeInForce",
    ".mftiko.td.v1.OrderStatus",
    ".mftiko.td.v1.OrderCause",
    ".mftiko.td.v1.ReconReason",
];

fn require_present(
    messages: &BTreeSet<String>,
    enums: &BTreeSet<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut missing = Vec::new();
    for name in REQUIRED_MESSAGES {
        if !messages.contains(*name) {
            missing.push((*name).to_owned());
        }
    }
    for name in REQUIRED_ENUMS {
        if !enums.contains(*name) {
            missing.push((*name).to_owned());
        }
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!("proto closure is missing {}", missing.join(", ")).into())
    }
}

fn is_seed_package(package: &str) -> bool {
    package == "mftiko.md.v1" || package == "mftiko.td.v1"
}

fn index_message<'a>(
    package: &str,
    parent: Option<&str>,
    message: &'a DescriptorProto,
    messages: &mut BTreeMap<String, &'a DescriptorProto>,
    maps: &mut BTreeMap<String, &'a DescriptorProto>,
    nested: &mut BTreeSet<String>,
    packages: &mut BTreeMap<String, String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let name = required_name(message.name.as_deref(), "message")?;
    let full = match parent {
        Some(parent) => format!("{parent}.{name}"),
        None => format!(".{package}.{name}"),
    };
    if message
        .options
        .as_ref()
        .and_then(|options| options.map_entry)
        .unwrap_or(false)
    {
        maps.insert(full, message);
        return Ok(());
    }
    if parent.is_some() {
        nested.insert(full.clone());
        for child in &message.nested_type {
            index_message(
                package,
                Some(&full),
                child,
                messages,
                maps,
                nested,
                packages,
            )?;
        }
        for enum_def in &message.enum_type {
            let enum_name = required_name(enum_def.name.as_deref(), "enum")?;
            nested.insert(format!("{full}.{enum_name}"));
        }
        return Ok(());
    }
    packages.insert(full.clone(), package.to_owned());
    messages.insert(full.clone(), message);
    for child in &message.nested_type {
        index_message(
            package,
            Some(&full),
            child,
            messages,
            maps,
            nested,
            packages,
        )?;
    }
    for enum_def in &message.enum_type {
        let enum_name = required_name(enum_def.name.as_deref(), "enum")?;
        nested.insert(format!("{full}.{enum_name}"));
    }
    Ok(())
}

fn collect_refs(
    message: &DescriptorProto,
    index: &Index<'_>,
    queue: &mut VecDeque<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    for field in &message.field {
        for full in type_refs(field, index)? {
            queue.push_back(full);
        }
    }
    Ok(())
}

fn type_refs(
    field: &FieldDescriptorProto,
    index: &Index<'_>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut out = Vec::new();
    let kind_name = field_type_name(field);
    let ty = field.r#type.unwrap_or(0);
    let repeated = field.label == Some(LABEL_REPEATED);
    if repeated
        && ty == TYPE_MESSAGE
        && let Some(name) = &kind_name
        && index.maps.contains_key(name)
    {
        let entry = index.maps[name];
        for child in &entry.field {
            if child.number == Some(2) {
                out.extend(type_refs(child, index)?);
            }
        }
        return Ok(out);
    }
    if ty == TYPE_GROUP {
        return Err(format!("需要決定: proto group field {}", field_label(field)).into());
    }
    if (matches!(ty, TYPE_MESSAGE | TYPE_ENUM) || (ty == 0 && kind_name.is_some()))
        && let Some(name) = kind_name
    {
        out.push(name);
    }
    Ok(out)
}

fn message_def(full: &str, index: &Index<'_>) -> Result<MsgDef, Box<dyn std::error::Error>> {
    let message = index.messages[full];
    let package = &index.packages[full];
    let name = required_name(message.name.as_deref(), "message")?;
    let module = rust_module(package);
    let mut branch_count = BTreeMap::<i32, usize>::new();
    for field in &message.field {
        if let Some(index_of) = real_oneof_index(field) {
            *branch_count.entry(index_of).or_default() += 1;
        }
    }
    let mut slots = Vec::new();
    for field in &message.field {
        if let Some(index_of) = real_oneof_index(field) {
            let oneof_name = oneof_name(message, index_of)?;
            let rust_field = to_snake(&oneof_name);
            let rust_enum = format!(
                "{module}::{}::{}",
                to_snake(&name),
                to_upper_camel(&oneof_name)
            );
            let py_name = field_py_name(field)?;
            slots.push(Slot::Branch(BranchDef {
                py_name,
                rust_field,
                rust_enum: rust_enum.clone(),
                variant: to_upper_camel(&field_py_name(field)?),
                kind: element_kind(field, index)?,
            }));
            let left = branch_count.get_mut(&index_of).expect("counted");
            *left -= 1;
            if *left == 0 {
                let arms = message
                    .field
                    .iter()
                    .filter(|candidate| real_oneof_index(candidate) == Some(index_of))
                    .map(|candidate| {
                        let py_name = field_py_name(candidate)?;
                        Ok((to_upper_camel(&py_name), py_name))
                    })
                    .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
                slots.push(Slot::OneofName(OneofName {
                    py_name: oneof_name.clone(),
                    rust_field: to_snake(&oneof_name),
                    rust_enum,
                    arms,
                }));
            }
        } else {
            slots.push(Slot::Plain(FieldDef {
                py_name: field_py_name(field)?,
                rust_field: to_snake(&field_py_name(field)?),
                kind: plain_kind(field, index)?,
            }));
        }
    }
    Ok(MsgDef {
        py_module: py_module(package)?,
        name,
        rust_type: format!(
            "{module}::{}",
            required_name(message.name.as_deref(), "message")?
        ),
        rust_struct: rust_struct(package, message.name.as_deref().unwrap_or_default()),
        slots,
    })
}

fn plain_kind(
    field: &FieldDescriptorProto,
    index: &Index<'_>,
) -> Result<Kind, Box<dyn std::error::Error>> {
    let repeated = field.label == Some(LABEL_REPEATED);
    if repeated {
        if field.r#type == Some(TYPE_MESSAGE)
            && let Some(name) = field_type_name(field)
            && let Some(entry) = index.maps.get(&name)
        {
            return map_kind(entry, index);
        }
        return Ok(Kind::Repeated(Box::new(element_kind(field, index)?)));
    }
    let mut kind = element_kind(field, index)?;
    let optional = field.proto3_optional.unwrap_or(false);
    match &mut kind {
        Kind::Scalar { optional: flag, .. }
        | Kind::EnumRef { optional: flag, .. }
        | Kind::Bytes { optional: flag } => {
            *flag = optional;
        }
        Kind::MessageRef { .. } => {}
        Kind::Repeated(_) | Kind::Map { .. } => {}
    }
    Ok(kind)
}

fn map_kind(
    entry: &DescriptorProto,
    index: &Index<'_>,
) -> Result<Kind, Box<dyn std::error::Error>> {
    let key_field = entry
        .field
        .iter()
        .find(|field| field.number == Some(1))
        .ok_or("map entry is missing key")?;
    let value_field = entry
        .field
        .iter()
        .find(|field| field.number == Some(2))
        .ok_or("map entry is missing value")?;
    let key = scalar_of(key_field.r#type.unwrap_or(0)).ok_or_else(|| {
        format!(
            "需要決定: map key {}",
            field_type_name(key_field).unwrap_or_else(|| field_label(key_field))
        )
    })?;
    Ok(Kind::Map {
        key,
        value: Box::new(element_kind(value_field, index)?),
    })
}

fn element_kind(
    field: &FieldDescriptorProto,
    index: &Index<'_>,
) -> Result<Kind, Box<dyn std::error::Error>> {
    let ty = field.r#type.unwrap_or(0);
    let type_name = field_type_name(field);
    if ty == TYPE_GROUP {
        return Err(format!("需要決定: proto group field {}", field_label(field)).into());
    }
    if ty == TYPE_BYTES {
        return Ok(Kind::Bytes { optional: false });
    }
    if ty == TYPE_MESSAGE
        || (ty == 0
            && type_name
                .as_ref()
                .is_some_and(|name| index.messages.contains_key(name)))
    {
        let full =
            type_name.ok_or_else(|| format!("message field {} has no type", field_label(field)))?;
        let package = index
            .packages
            .get(&full)
            .ok_or_else(|| format!("unknown message {full}"))?;
        let name = full.rsplit('.').next().unwrap_or_default().to_owned();
        return Ok(Kind::MessageRef {
            rust_struct: rust_struct(package, &name),
            py_module: py_module(package)?,
            name,
        });
    }
    if ty == TYPE_ENUM
        || (ty == 0
            && type_name
                .as_ref()
                .is_some_and(|name| index.enums.contains_key(name)))
    {
        let full =
            type_name.ok_or_else(|| format!("enum field {} has no type", field_label(field)))?;
        let package = index
            .packages
            .get(&full)
            .ok_or_else(|| format!("unknown enum {full}"))?;
        let name = full.rsplit('.').next().unwrap_or_default().to_owned();
        return Ok(Kind::EnumRef {
            py_module: py_module(package)?,
            name,
            optional: false,
        });
    }
    let scalar =
        scalar_of(ty).ok_or_else(|| format!("unsupported field {}", field_label(field)))?;
    Ok(Kind::Scalar {
        scalar,
        optional: false,
    })
}

fn scalar_of(ty: i32) -> Option<Scalar> {
    Some(match Type::try_from(ty).ok()? {
        Type::Double => Scalar::F64,
        Type::Float => Scalar::F32,
        Type::Int32 | Type::Sfixed32 | Type::Sint32 => Scalar::I32,
        Type::Int64 | Type::Sfixed64 | Type::Sint64 => Scalar::I64,
        Type::Uint32 | Type::Fixed32 => Scalar::U32,
        Type::Uint64 | Type::Fixed64 => Scalar::U64,
        Type::Bool => Scalar::Bool,
        Type::String => Scalar::String,
        _ => return None,
    })
}

fn real_oneof_index(field: &FieldDescriptorProto) -> Option<i32> {
    if field.proto3_optional.unwrap_or(false) {
        None
    } else {
        field.oneof_index
    }
}

fn oneof_name(message: &DescriptorProto, index: i32) -> Result<String, Box<dyn std::error::Error>> {
    message
        .oneof_decl
        .get(usize::try_from(index).unwrap_or(usize::MAX))
        .and_then(|oneof| oneof.name.clone())
        .ok_or_else(|| format!("oneof index {index}").into())
}

fn field_py_name(field: &FieldDescriptorProto) -> Result<String, Box<dyn std::error::Error>> {
    field
        .name
        .clone()
        .ok_or_else(|| "field without a name".into())
}

fn field_type_name(field: &FieldDescriptorProto) -> Option<String> {
    let name = field.type_name.as_ref()?;
    if name.starts_with('.') {
        Some(name.clone())
    } else {
        Some(format!(".{name}"))
    }
}

fn field_label(field: &FieldDescriptorProto) -> String {
    field.name.clone().unwrap_or_else(|| "<unnamed>".to_owned())
}

fn required_name(name: Option<&str>, what: &str) -> Result<String, Box<dyn std::error::Error>> {
    name.map(str::to_owned)
        .ok_or_else(|| format!("{what} without a name").into())
}

fn py_module(package: &str) -> Result<String, Box<dyn std::error::Error>> {
    let Some(rest) = package.strip_prefix("mftiko.") else {
        return Err(format!("需要決定: package {package} is outside mftiko").into());
    };
    let Some((name, "v1")) = rest.split_once('.') else {
        return Err(format!("需要決定: package {package} is not a v1 package").into());
    };
    if !is_ident(name) {
        return Err(format!("需要決定: package {package}").into());
    }
    Ok(format!("mftik.{name}.v1"))
}

fn rust_module(package: &str) -> String {
    format!("::mftik_proto::wire::{}", package.replace('.', "::"))
}

fn rust_struct(package: &str, name: &str) -> String {
    let mut out = String::from("Py");
    for part in package.split('.').skip(1) {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            out.extend(first.to_uppercase());
            out.push_str(chars.as_str());
        }
    }
    out.push_str(name);
    out
}

fn is_ident(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        _ => false,
    }
}

fn to_snake(name: &str) -> String {
    sanitize_identifier(&name.to_snake_case())
}

fn to_upper_camel(name: &str) -> String {
    sanitize_identifier(&name.to_upper_camel_case())
}

fn sanitize_identifier(ident: &str) -> String {
    match ident {
        "as" | "break" | "const" | "continue" | "else" | "enum" | "false" | "fn" | "for" | "if"
        | "impl" | "in" | "let" | "loop" | "match" | "mod" | "move" | "mut" | "pub" | "ref"
        | "return" | "static" | "struct" | "trait" | "true" | "type" | "unsafe" | "use"
        | "where" | "while" | "dyn" | "abstract" | "become" | "box" | "do" | "final" | "macro"
        | "override" | "priv" | "typeof" | "unsized" | "virtual" | "yield" | "async" | "await"
        | "try" | "gen" => format!("r#{ident}"),
        "_" | "super" | "self" | "Self" | "extern" | "crate" => format!("{ident}_"),
        _ if ident.starts_with(|c: char| c.is_numeric()) => format!("_{ident}"),
        _ => ident.to_owned(),
    }
}

fn emit_rust(model: &Model) -> String {
    let mut out = String::new();
    for message in &model.messages {
        out.push_str(&emit_message(message));
    }
    out.push_str(&emit_register(model));
    out
}

fn emit_message(message: &MsgDef) -> String {
    let mut body = format!(
        "#[pyclass(frozen, eq, module = \"{}\", name = \"{}\")]\n#[derive(Clone, PartialEq)]\npub struct {} {{\n    pub(crate) inner: {},\n}}\n\n#[pymethods]\nimpl {} {{\n",
        message.py_module,
        message.name,
        message.rust_struct,
        message.rust_type,
        message.rust_struct
    );
    for slot in &message.slots {
        body.push_str(&emit_slot(slot));
    }
    body.push_str(&emit_repr(message));
    body.push_str("}\n\n");
    body.push_str(&format!(
        "impl From<{}> for {} {{\n    fn from(inner: {}) -> Self {{\n        Self {{ inner }}\n    }}\n}}\n\n",
        message.rust_type, message.rust_struct, message.rust_type
    ));
    body.push_str(&format!(
        "impl ToPyMessage for {} {{\n    fn to_py(self, py: Python<'_>) -> PyResult<Py<PyAny>> {{\n        Ok(Py::new(py, {} {{ inner: self }})?.into_any())\n    }}\n}}\n\n",
        message.rust_type, message.rust_struct
    ));
    body
}

fn emit_slot(slot: &Slot) -> String {
    match slot {
        Slot::Plain(field) => emit_plain(field),
        Slot::Branch(branch) => emit_branch(branch),
        Slot::OneofName(oneof) => emit_oneof_name(oneof),
    }
}

fn emit_plain(field: &FieldDef) -> String {
    let name = &field.py_name;
    let rust_field = &field.rust_field;
    let fn_name = &field.rust_field;
    match &field.kind {
        Kind::Scalar {
            scalar,
            optional: false,
        } => format!(
            "    #[getter]\n    #[pyo3(name = \"{name}\")]\n    fn {fn_name}(&self) -> {} {{\n        self.inner.{rust_field}.clone()\n    }}\n\n",
            scalar.rust()
        ),
        Kind::Scalar {
            scalar,
            optional: true,
        } => format!(
            "    #[getter]\n    #[pyo3(name = \"{name}\")]\n    fn {fn_name}(&self) -> Option<{}> {{\n        self.inner.{rust_field}.clone()\n    }}\n\n",
            scalar.rust()
        ),
        Kind::EnumRef {
            py_module,
            name: enum_name,
            optional: false,
        } => format!(
            "    #[getter]\n    #[pyo3(name = \"{name}\")]\n    fn {fn_name}(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {{\n        enum_value(py, \"{py_module}\", \"{enum_name}\", self.inner.{rust_field})\n    }}\n\n"
        ),
        Kind::EnumRef {
            py_module,
            name: enum_name,
            optional: true,
        } => format!(
            "    #[getter]\n    #[pyo3(name = \"{name}\")]\n    fn {fn_name}(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {{\n        match self.inner.{rust_field} {{\n            Some(value) => enum_value(py, \"{py_module}\", \"{enum_name}\", value),\n            None => Ok(py.None()),\n        }}\n    }}\n\n"
        ),
        Kind::MessageRef { rust_struct, .. } => format!(
            "    #[getter]\n    #[pyo3(name = \"{name}\")]\n    fn {fn_name}(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {{\n        match &self.inner.{rust_field} {{\n            Some(value) => Ok(Py::new(py, {rust_struct} {{ inner: value.clone() }})?.into_any()),\n            None => Ok(py.None()),\n        }}\n    }}\n\n"
        ),
        Kind::Bytes { optional: false } => format!(
            "    #[getter]\n    #[pyo3(name = \"{name}\")]\n    fn {fn_name}(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {{\n        Ok(PyBytes::new(py, &self.inner.{rust_field}).unbind())\n    }}\n\n"
        ),
        Kind::Bytes { optional: true } => format!(
            "    #[getter]\n    #[pyo3(name = \"{name}\")]\n    fn {fn_name}(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {{\n        match &self.inner.{rust_field} {{\n            Some(value) => Ok(PyBytes::new(py, value).unbind().into_any()),\n            None => Ok(py.None()),\n        }}\n    }}\n\n"
        ),
        Kind::Repeated(inner) => emit_repeated(name, fn_name, rust_field, inner),
        Kind::Map { key: _, value } => emit_map(name, fn_name, rust_field, value),
    }
}

fn emit_repeated(name: &str, fn_name: &str, rust_field: &str, inner: &Kind) -> String {
    let fill = match inner {
        Kind::MessageRef { rust_struct, .. } => format!(
            "        for item in &self.inner.{rust_field} {{\n            items.push(Py::new(py, {rust_struct} {{ inner: item.clone() }})?);\n        }}\n"
        ),
        Kind::EnumRef {
            py_module,
            name: enum_name,
            ..
        } => format!(
            "        for item in &self.inner.{rust_field} {{\n            items.push(enum_value(py, \"{py_module}\", \"{enum_name}\", *item)?);\n        }}\n"
        ),
        Kind::Bytes { .. } => format!(
            "        for item in &self.inner.{rust_field} {{\n            items.push(PyBytes::new(py, item).unbind());\n        }}\n"
        ),
        Kind::Scalar { .. } => format!(
            "        for item in &self.inner.{rust_field} {{\n            items.push(item.clone());\n        }}\n"
        ),
        Kind::Repeated(_) | Kind::Map { .. } => {
            "        compile_error!(\"nested repeated\");\n".to_owned()
        }
    };
    format!(
        "    #[getter]\n    #[pyo3(name = \"{name}\")]\n    fn {fn_name}(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {{\n        let mut items = Vec::new();\n{fill}        Ok(PyTuple::new(py, items)?.unbind())\n    }}\n\n"
    )
}

fn emit_map(name: &str, fn_name: &str, rust_field: &str, value: &Kind) -> String {
    let set = match value {
        Kind::MessageRef { rust_struct, .. } => format!(
            "            dict.set_item(key, Py::new(py, {rust_struct} {{ inner: value.clone() }})?)?;\n"
        ),
        Kind::EnumRef {
            py_module,
            name: enum_name,
            ..
        } => format!(
            "            dict.set_item(key, enum_value(py, \"{py_module}\", \"{enum_name}\", *value)?)?;\n"
        ),
        Kind::Bytes { .. } => {
            "            dict.set_item(key, PyBytes::new(py, value))?;\n".to_owned()
        }
        Kind::Scalar { .. } => "            dict.set_item(key, value)?;\n".to_owned(),
        Kind::Repeated(_) | Kind::Map { .. } => {
            "            compile_error!(\"nested map\");\n".to_owned()
        }
    };
    format!(
        "    #[getter]\n    #[pyo3(name = \"{name}\")]\n    fn {fn_name}(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {{\n        let dict = PyDict::new(py);\n        let mut pairs: Vec<_> = self.inner.{rust_field}.iter().collect();\n        pairs.sort_by(|left, right| left.0.cmp(right.0));\n        for (key, value) in pairs {{\n{set}        }}\n        Ok(dict.unbind())\n    }}\n\n"
    )
}

fn emit_branch(branch: &BranchDef) -> String {
    let name = &branch.py_name;
    let fn_name = &branch.rust_field_fn();
    let field = &branch.rust_field;
    let enum_path = &branch.rust_enum;
    let variant = &branch.variant;
    let convert = match &branch.kind {
        Kind::MessageRef { rust_struct, .. } => {
            format!("Ok(Py::new(py, {rust_struct} {{ inner: value.clone() }})?.into_any())")
        }
        Kind::EnumRef {
            py_module,
            name: enum_name,
            ..
        } => {
            format!("enum_value(py, \"{py_module}\", \"{enum_name}\", *value)")
        }
        Kind::Bytes { .. } => "Ok(PyBytes::new(py, value).unbind().into_any())".to_owned(),
        Kind::Scalar { .. } => {
            "Ok(value.clone().into_pyobject(py)?.unbind().into_any())".to_owned()
        }
        Kind::Repeated(_) | Kind::Map { .. } => "compile_error!(\"oneof container\")".to_owned(),
    };
    format!(
        "    #[getter]\n    #[pyo3(name = \"{name}\")]\n    fn {fn_name}(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {{\n        match &self.inner.{field} {{\n            Some({enum_path}::{variant}(value)) => {convert},\n            _ => Ok(py.None()),\n        }}\n    }}\n\n"
    )
}

impl BranchDef {
    fn rust_field_fn(&self) -> String {
        to_snake(&self.py_name)
    }
}

fn emit_oneof_name(oneof: &OneofName) -> String {
    let name = &oneof.py_name;
    let fn_name = to_snake(name);
    let field = &oneof.rust_field;
    let enum_path = &oneof.rust_enum;
    let mut arms = String::new();
    for (variant, py_name) in &oneof.arms {
        arms.push_str(&format!(
            "            Some({enum_path}::{variant}(_)) => Some(\"{py_name}\"),\n"
        ));
    }
    format!(
        "    #[getter]\n    #[pyo3(name = \"{name}\")]\n    fn {fn_name}(&self) -> Option<&'static str> {{\n        match &self.inner.{field} {{\n{arms}            None => None,\n        }}\n    }}\n\n"
    )
}

fn emit_repr(message: &MsgDef) -> String {
    let mut parts = String::new();
    for slot in &message.slots {
        let (py_name, call) = match slot {
            Slot::Plain(field) => (
                field.py_name.as_str(),
                call_expr(&field.rust_field, &field.kind),
            ),
            Slot::Branch(branch) => (
                branch.py_name.as_str(),
                format!("self.{}(py)?", branch.rust_field_fn()),
            ),
            Slot::OneofName(oneof) => (
                oneof.py_name.as_str(),
                format!("self.{}()", to_snake(&oneof.py_name)),
            ),
        };
        parts.push_str(&format!(
            "        parts.push(format!(\"{{}}={{}}\", \"{py_name}\", repr_value(py, {call})?));\n"
        ));
    }
    format!(
        "    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {{\n        let mut parts = Vec::<String>::new();\n{parts}        Ok(format!(\"{}({{}})\", parts.join(\", \")))\n    }}\n",
        message.name
    )
}

fn call_expr(fn_name: &str, kind: &Kind) -> String {
    let needs_py = match kind {
        Kind::Scalar { .. } => false,
        Kind::EnumRef { .. }
        | Kind::MessageRef { .. }
        | Kind::Bytes { .. }
        | Kind::Repeated(_)
        | Kind::Map { .. } => true,
    };
    if needs_py {
        format!("self.{fn_name}(py)?")
    } else {
        format!("self.{fn_name}()")
    }
}

fn emit_register(model: &Model) -> String {
    let mut modules: BTreeMap<&str, (Vec<&EnumDef>, Vec<&MsgDef>)> = BTreeMap::new();
    for enum_def in &model.enums {
        modules
            .entry(&enum_def.py_module)
            .or_default()
            .0
            .push(enum_def);
    }
    for message in &model.messages {
        modules
            .entry(&message.py_module)
            .or_default()
            .1
            .push(message);
    }
    let mut body = String::from(
        "pub fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {\n    let sys_modules = py.import(\"sys\")?.getattr(\"modules\")?;\n    let modules = sys_modules.cast::<PyDict>()?;\n",
    );
    let mut parents = BTreeSet::new();
    for module in modules.keys() {
        if let Some(parent) = module.rsplit_once('.').map(|(parent, _)| parent) {
            parents.insert(parent.to_owned());
        }
    }
    for parent in &parents {
        let short = parent.rsplit('.').next().unwrap_or(parent);
        let var = parent.replace('.', "_");
        body.push_str(&format!(
            "    let {var} = PyModule::new(py, \"{short}\")?;\n    {var}.setattr(\"__name__\", \"{parent}\")?;\n    {var}.setattr(\"__package__\", \"{parent}\")?;\n    {var}.setattr(\"__path__\", PyList::empty(py))?;\n"
        ));
    }
    for (module, (enums, messages)) in &modules {
        let parent = module
            .rsplit_once('.')
            .map(|(parent, _)| parent)
            .unwrap_or(module);
        let parent_var = parent.replace('.', "_");
        let var = module.replace('.', "_");
        body.push_str(&format!(
            "    let {var} = PyModule::new(py, \"v1\")?;\n    {var}.setattr(\"__name__\", \"{module}\")?;\n    {var}.setattr(\"__package__\", \"{module}\")?;\n"
        ));
        if !enums.is_empty() {
            let code = enum_python(enums);
            body.push_str(&format!(
                "    run_enums(py, &{var}, {code})?;\n    if {var}.dict().get_item(\"__builtins__\")?.is_some() {{\n        {var}.dict().del_item(\"__builtins__\")?;\n    }}\n"
            ));
            for enum_def in enums {
                body.push_str(&format!(
                    "    {var}.getattr(\"{}\")?.setattr(\"__module__\", \"{module}\")?;\n",
                    enum_def.name
                ));
            }
        }
        for message in messages {
            body.push_str(&format!(
                "    {var}.add_class::<{}>()?;\n",
                message.rust_struct
            ));
        }
        body.push_str(&format!(
            "    {parent_var}.add_submodule(&{var})?;\n    {parent_var}.setattr(\"v1\", &{var})?;\n    modules.set_item(\"{module}\", &{var})?;\n"
        ));
    }
    for parent in &parents {
        let parent_var = parent.replace('.', "_");
        let short = parent.rsplit('.').next().unwrap_or(parent);
        body.push_str(&format!(
            "    root.add_submodule(&{parent_var})?;\n    root.setattr(\"{short}\", &{parent_var})?;\n    modules.set_item(\"{parent}\", &{parent_var})?;\n"
        ));
    }
    body.push_str("    Ok(())\n}\n");
    body
}

fn enum_python(enums: &[&EnumDef]) -> String {
    let mut code = String::from("import enum as _mftik_enum\n");
    code.push_str("def _mftik_reject_bool(cls):\n");
    code.push_str("    _orig = cls.__new__\n");
    code.push_str("    def _new(cls, value):\n");
    code.push_str("        if isinstance(value, bool):\n");
    code.push_str("            raise ValueError('bool is not an enum value')\n");
    code.push_str("        return _orig(cls, value)\n");
    code.push_str("    cls.__new__ = _new\n");
    code.push_str("    return cls\n");
    for enum_def in enums {
        code.push_str(&format!("class {}(_mftik_enum.IntEnum):\n", enum_def.name));
        for (name, value) in &enum_def.values {
            code.push_str(&format!("    {name} = {value}\n"));
        }
        code.push_str(
            "    @classmethod\n    def _missing_(cls, value):\n        if isinstance(value, bool) or not isinstance(value, int):\n            return None\n        member = int.__new__(cls, value)\n        member._name_ = str(value)\n        member._value_ = value\n        return member\n",
        );
        code.push_str(&format!("_mftik_reject_bool({})\n", enum_def.name));
    }
    code.push_str("del _mftik_reject_bool\ndel _mftik_enum\n");
    format!("r###\"{code}\"###")
}

fn write_pyi(root: &Path, model: &Model) -> Result<(), Box<dyn std::error::Error>> {
    let mut by_module: BTreeMap<&str, (Vec<&EnumDef>, Vec<&MsgDef>)> = BTreeMap::new();
    for enum_def in &model.enums {
        by_module
            .entry(&enum_def.py_module)
            .or_default()
            .0
            .push(enum_def);
    }
    for message in &model.messages {
        by_module
            .entry(&message.py_module)
            .or_default()
            .1
            .push(message);
    }
    for (module, (enums, messages)) in &by_module {
        let relative = module.trim_start_matches("mftik.");
        let (parent, _) = relative.rsplit_once('.').ok_or("module has no parent")?;
        let dir = root.join(parent);
        fs::create_dir_all(&dir)?;
        fs::write(
            dir.join("__init__.pyi"),
            "from . import v1 as v1\n\n__all__ = [\"v1\"]\n",
        )?;
        fs::write(dir.join("v1.pyi"), emit_pyi(module, enums, messages)?)?;
    }
    Ok(())
}

fn emit_pyi(
    module: &str,
    enums: &[&EnumDef],
    messages: &[&MsgDef],
) -> Result<String, Box<dyn std::error::Error>> {
    let mut imports = BTreeSet::new();
    let mut body = String::new();
    for enum_def in enums {
        body.push_str(&format!("class {}(enum.IntEnum):\n", enum_def.name));
        for (name, value) in &enum_def.values {
            body.push_str(&format!("    {name} = {value}\n"));
        }
        body.push_str(&format!(
            "    @classmethod\n    def _missing_(cls, value: object) -> {0} | None: ...\n\n",
            enum_def.name
        ));
    }
    let mut exports = Vec::new();
    for message in messages {
        exports.push(message.name.as_str());
        body.push_str(&format!("@final\nclass {}:\n", message.name));
        if message.slots.is_empty() {
            body.push_str("    pass\n");
        }
        for slot in &message.slots {
            let (py_name, kind, optional_branch) = match slot {
                Slot::Plain(field) => (field.py_name.as_str(), &field.kind, false),
                Slot::Branch(branch) => (branch.py_name.as_str(), &branch.kind, true),
                Slot::OneofName(oneof) => {
                    body.push_str(&format!("    {}: str | None\n", oneof.py_name));
                    continue;
                }
            };
            body.push_str(&format!(
                "    {py_name}: {}\n",
                py_annotation(module, kind, optional_branch, false, &mut imports)?
            ));
        }
        body.push_str("    def __eq__(self, other: object, /) -> bool: ...\n");
        body.push_str("    def __repr__(self) -> str: ...\n\n");
    }
    let mut header = String::from("from __future__ import annotations\n\n");
    if !enums.is_empty() {
        header.push_str("import enum\n");
    }
    if !messages.is_empty() {
        header.push_str("from typing import final\n");
    }
    for (import_module, names) in import_groups(&imports) {
        header.push_str(&format!(
            "from {import_module} import {}\n",
            names.join(", ")
        ));
    }
    if !exports.is_empty() {
        header.push_str("\n__all__ = [");
        for (index, name) in exports.iter().enumerate() {
            if index > 0 {
                header.push_str(", ");
            }
            header.push_str(&format!("\"{name}\""));
        }
        header.push_str("]\n");
    }
    header.push('\n');
    header.push_str(&body);
    Ok(header)
}

fn import_groups(imports: &BTreeSet<(String, String)>) -> BTreeMap<String, Vec<String>> {
    let mut groups = BTreeMap::<String, Vec<String>>::new();
    for (module, name) in imports {
        groups.entry(module.clone()).or_default().push(name.clone());
    }
    groups
}

fn py_annotation(
    module: &str,
    kind: &Kind,
    branch: bool,
    element: bool,
    imports: &mut BTreeSet<(String, String)>,
) -> Result<String, Box<dyn std::error::Error>> {
    let rendered = match kind {
        Kind::Scalar { scalar, optional } => {
            let text = scalar.py().to_owned();
            if *optional {
                format!("{text} | None")
            } else {
                text
            }
        }
        Kind::Bytes { optional } => {
            if *optional {
                "bytes | None".to_owned()
            } else {
                "bytes".to_owned()
            }
        }
        Kind::EnumRef {
            py_module,
            name,
            optional,
        } => {
            note_import(module, py_module, name, imports);
            if *optional {
                format!("{name} | None")
            } else {
                name.clone()
            }
        }
        Kind::MessageRef {
            py_module, name, ..
        } => {
            note_import(module, py_module, name, imports);
            if element {
                name.clone()
            } else {
                format!("{name} | None")
            }
        }
        Kind::Repeated(inner) => {
            let inner = py_annotation(module, inner, false, true, imports)?;
            format!("tuple[{inner}, ...]")
        }
        Kind::Map { key, value } => {
            let value = py_annotation(module, value, false, true, imports)?;
            format!("dict[{}, {value}]", key.py())
        }
    };
    if branch && !rendered.ends_with("| None") {
        Ok(format!("{rendered} | None"))
    } else {
        Ok(rendered)
    }
}

fn note_import(
    module: &str,
    py_module: &str,
    name: &str,
    imports: &mut BTreeSet<(String, String)>,
) {
    if module != py_module {
        imports.insert((py_module.to_owned(), name.to_owned()));
    }
}
