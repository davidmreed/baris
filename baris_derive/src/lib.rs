extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Lit, Meta, MetaNameValue, NestedMeta};

#[proc_macro_derive(SObjectRepresentation, attributes(baris))]
pub fn sobject_representation_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let ident = ast.ident;
    let mut name = ident.to_string();

    const USAGE: &str = "[#baris] requires an API name argument: api_name(\"Name\")";

    // Were we given an api_name attribute?
    for attr in ast.attrs {
        if attr.path.is_ident("baris") {
            let meta = attr.parse_meta().expect(USAGE);
            match meta {
                Meta::List(list) => {
                    let content = list.nested.first().expect(USAGE);
                    match content {
                        NestedMeta::Meta(Meta::NameValue(MetaNameValue {
                            lit: Lit::Str(api_name),
                            path: _,
                            eq_token: _,
                        })) => name = api_name.value(),
                        _ => panic!("{}", USAGE),
                    };
                }
                _ => panic!("{}", USAGE),
            }
        }
    }

    let gen = quote! {
        impl baris::data::traits::SObjectWithId for #ident {

            fn get_id(&self) -> FieldValue {
                match self.get_opt_id() {
                    Some(id) => FieldValue::Id(id),
                    None => FieldValue::Null
                }
            }

            fn set_id(&mut self, id: FieldValue) -> Result<()> {
                match id {
                    FieldValue::Id(id) => {self.set_opt_id(Some(id))?; Ok(())},
                    FieldValue::Null => {self.set_opt_id(None)?; Ok(())},
                    _ => Err(SalesforceError::UnsupportedId.into())
                }
            }

            fn get_opt_id(&self) -> Option<baris::data::types::SalesforceId> {
                self.id
            }

            fn set_opt_id(&mut self, id: Option<baris::data::types::SalesforceId>) -> Result<()> {
                self.id = id;
                Ok(())
            }
        }

        impl baris::data::traits::SingleTypedSObject for #ident {
            fn get_type_api_name() -> &'static str {
                #name
            }
        }

        impl baris::data::traits::SObjectBase for #ident {}
    };
    gen.into()
}
