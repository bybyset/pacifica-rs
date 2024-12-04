mod expand;

use proc_macro::TokenStream;
use syn::parse_macro_input;

#[proc_macro]
pub fn expand(item: TokenStream) -> TokenStream {
    let repeat = parse_macro_input!(item as expand::Expand);
    repeat.render().into()
}