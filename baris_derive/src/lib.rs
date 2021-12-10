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
        impl SObjectRepresentation for #ident {
            fn get_id(&self) -> Option<SalesforceId> {
                self.id
            }

            fn set_id(&mut self, id: Option<SalesforceId>) {
                self.id = id;
            }
        }

        impl SingleTypedSObjectRepresentation for #ident {
            fn get_type_api_name(&self) -> &str {
                #name
            }
        }
    };
    gen.into()
}
