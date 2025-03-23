

#[macro_export]
macro_rules! declare_pacifica_types {
    // Add a trailing colon to    `declare_pacifica_types(MyType)`,
    // Make it the standard form: `declare_pacifica_types(MyType:)`.
    ($(#[$outer:meta])* $visibility:vis $id:ident) => {
        $crate::declare_pacifica_types!($(#[$outer])* $visibility $id:);
    };

    // The main entry of this macro
    ($(#[$outer:meta])* $visibility:vis $id:ident: $($(#[$inner:meta])* $type_id:ident = $type:ty),* $(,)? ) => {
        $(#[$outer])*
        #[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
        #[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
        $visibility struct $id {}

        impl $crate::TypeConfig for $id {
            // `expand!(KEYED, ...)` ignores the duplicates.
            // Thus by appending default types after user defined types,
            // the absent user defined types are filled with default types.
            $crate::pacifica_rs_macros::expand!(
                KEYED,
                (T, ATTR, V) => {ATTR type T = V;},
                $(($type_id, $(#[$inner])*, $type),)*

                Default types:
                (NodeId          , , $crate::StrNodeId                                   ),
                (AsyncRuntime    , , $crate::TokioRuntime              ),
                (LogStorage      , , $crate::storage::rocksdb_impl::RocksdbLogStore                  ),
                (SnapshotStorage , , $crate::storage::fs_impl::FsSnapshotStore ),
            );

        }
    };
}