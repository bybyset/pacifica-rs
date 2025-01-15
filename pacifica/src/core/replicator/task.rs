use crate::TypeConfig;

pub(crate) enum Task<C>
where
    C: TypeConfig, {


    Heartbeat {

    },

    Probe {

    },

    AppendLogEntries {

    },

    InstallSnapshot {

    },





}
