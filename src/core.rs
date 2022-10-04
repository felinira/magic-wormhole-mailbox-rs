use serde::{Deserialize, Serialize};
use std::borrow::Cow;

// the serialized forms of these variants are part of the wire protocol, so
// they must be spelled exactly as shown
#[derive(Debug, PartialEq, Eq, Copy, Clone, Deserialize, Serialize, derive_more::Display)]
pub enum Mood {
    #[serde(rename = "happy")]
    Happy,
    #[serde(rename = "lonely")]
    Lonely,
    #[serde(rename = "errory")]
    Errory,
    #[serde(rename = "scary")]
    Scared,
    #[serde(rename = "unwelcome")]
    Unwelcome,
}

/**
 * Wormhole configuration corresponding to an uppler layer protocol
 *
 * There are multiple different protocols built on top of the core
 * Wormhole protocol. They are identified by a unique URI-like ID string
 * (`AppID`), an URL to find the rendezvous server (might be shared among
 * multiple protocols), and client implementations also have a "version"
 * data to do protocol negotiation.
 *
 * See [`crate::transfer::APP_CONFIG`], which entails
 */
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct AppConfig<V> {
    pub id: AppID,
    pub rendezvous_url: Cow<'static, str>,
    pub app_version: V,
}

impl<V> AppConfig<V> {
    pub fn id(mut self, id: AppID) -> Self {
        self.id = id;
        self
    }

    pub fn rendezvous_url(mut self, rendezvous_url: Cow<'static, str>) -> Self {
        self.rendezvous_url = rendezvous_url;
        self
    }
}

impl<V: serde::Serialize> AppConfig<V> {
    pub fn app_version(mut self, app_version: V) -> Self {
        self.app_version = app_version;
        self
    }
}

/// Newtype wrapper for application IDs
///
/// The application ID is a string that scopes all commands
/// to that name, effectively separating different protocols
/// on the same rendezvous server.
#[derive(
    PartialEq, Eq, Clone, Debug, Deserialize, Serialize, derive_more::Display, derive_more::Deref,
)]
#[deref(forward)]
pub struct AppID(#[deref] pub Cow<'static, str>);

impl AppID {
    pub fn new(id: impl Into<Cow<'static, str>>) -> Self {
        AppID(id.into())
    }
}

impl From<String> for AppID {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

// MySide is used for the String that we send in all our outbound messages
#[derive(
    PartialEq, Eq, Clone, Debug, Deserialize, Serialize, derive_more::Display, derive_more::Deref,
)]
#[serde(transparent)]
#[display(fmt = "MySide({})", "&*_0")]
pub struct MySide(EitherSide);

impl MySide {
    pub fn generate() -> MySide {
        use rand::{rngs::OsRng, RngCore};

        let mut bytes: [u8; 5] = [0; 5];
        OsRng.fill_bytes(&mut bytes);

        MySide(EitherSide(hex::encode(bytes)))
    }

    // It's a minor type system feature that converting an arbitrary string into MySide is hard.
    // This prevents it from getting swapped around with TheirSide.
    #[cfg(test)]
    pub fn unchecked_from_string(s: String) -> MySide {
        MySide(EitherSide(s))
    }
}

// TheirSide is used for the string that arrives inside inbound messages
#[derive(
    PartialEq, Eq, Clone, Debug, Deserialize, Serialize, derive_more::Display, derive_more::Deref,
)]
#[serde(transparent)]
#[display(fmt = "TheirSide({})", "&*_0")]
pub struct TheirSide(EitherSide);

impl<S: Into<String>> From<S> for TheirSide {
    fn from(s: S) -> TheirSide {
        TheirSide(EitherSide(s.into()))
    }
}

#[derive(
    PartialEq, Eq, Clone, Debug, Deserialize, Serialize, derive_more::Display, derive_more::Deref,
)]
#[serde(transparent)]
#[deref(forward)]
#[display(fmt = "{}", "&*_0")]
pub struct EitherSide(pub String);

impl<S: Into<String>> From<S> for EitherSide {
    fn from(s: S) -> EitherSide {
        EitherSide(s.into())
    }
}

#[derive(PartialEq, Eq, Clone, Debug, Hash, Deserialize, Serialize, derive_more::Display)]
#[serde(transparent)]
pub struct Phase(pub Cow<'static, str>);

impl Phase {
    pub const VERSION: Self = Phase(Cow::Borrowed("version"));
    pub const PAKE: Self = Phase(Cow::Borrowed("pake"));

    pub fn numeric(phase: u64) -> Self {
        Phase(phase.to_string().into())
    }

    pub fn is_version(&self) -> bool {
        self == &Self::VERSION
    }
    pub fn is_pake(&self) -> bool {
        self == &Self::PAKE
    }
    pub fn to_num(&self) -> Option<u64> {
        self.0.parse().ok()
    }
}

#[derive(PartialEq, Eq, Clone, Debug, Deserialize, Serialize, derive_more::Display)]
#[serde(transparent)]
pub struct Mailbox(pub String);

#[derive(
    PartialEq, Eq, Clone, Debug, Deserialize, Serialize, derive_more::Display, derive_more::Deref,
)]
#[serde(transparent)]
#[deref(forward)]
#[display(fmt = "{}", _0)]
pub struct Nameplate(pub String);
impl Nameplate {
    pub fn new(n: &str) -> Self {
        Nameplate(String::from(n))
    }
}

impl Into<String> for Nameplate {
    fn into(self) -> String {
        self.0
    }
}

/** A wormhole code à la 15-foo-bar
*
* The part until the first dash is called the "nameplate" and is purely numeric.
* The rest is the password and may be arbitrary, although dash-joining words from
* a wordlist is a common convention.
 */
#[derive(PartialEq, Eq, Clone, Debug, derive_more::Display, derive_more::Deref)]
#[display(fmt = "{}", _0)]
pub struct Code(pub String);

impl Code {
    pub fn new(nameplate: &Nameplate, password: &str) -> Self {
        Code(format!("{}-{}", nameplate, password))
    }

    pub fn split(&self) -> (Nameplate, String) {
        let mut iter = self.0.splitn(2, '-');
        let nameplate = Nameplate::new(iter.next().unwrap());
        let password = iter.next().unwrap();
        (nameplate, password.to_string())
    }

    pub fn nameplate(&self) -> Nameplate {
        Nameplate::new(self.0.split('-').next().unwrap())
    }
}
