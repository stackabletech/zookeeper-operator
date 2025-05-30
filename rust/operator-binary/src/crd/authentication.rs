use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    crd::authentication::core,
    schemars::{self, JsonSchema},
    versioned::versioned,
};

use crate::crd::ObjectRef;

const SUPPORTED_AUTHENTICATION_CLASS: [&str; 1] = ["TLS"];

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to retrieve AuthenticationClass [{}]", authentication_class))]
    AuthenticationClassRetrieval {
        source: stackable_operator::client::Error,
        authentication_class: ObjectRef<core::v1alpha1::AuthenticationClass>,
    },
    // TODO: Adapt message if multiple authentication classes are supported
    #[snafu(display(
        "only one authentication class is currently supported. Possible Authentication classes are {SUPPORTED_AUTHENTICATION_CLASS:?}"
    ))]
    MultipleAuthenticationClassesProvided,
    #[snafu(display(
        "failed to use authentication method [{method}] for authentication class [{authentication_class}] - supported mechanisms: {SUPPORTED_AUTHENTICATION_CLASS:?}",
    ))]
    AuthenticationMethodNotSupported {
        authentication_class: ObjectRef<core::v1alpha1::AuthenticationClass>,
        method: String,
    },
}

#[versioned(version(name = "v1alpha1"))]
pub mod versioned {
    #[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct ZookeeperAuthentication {
        /// The [AuthenticationClass](https://docs.stackable.tech/home/stable/concepts/authentication) to use.
        ///
        /// ## mTLS
        ///
        /// Only affects client connections. This setting controls:
        /// - If clients need to authenticate themselves against the server via TLS
        /// - Which ca.crt to use when validating the provided client certs
        ///
        /// This will override the server TLS settings (if set) in `spec.clusterConfig.tls.serverSecretClass`.
        pub authentication_class: String,
    }
}

#[derive(Clone, Debug)]
/// Helper struct that contains resolved AuthenticationClasses to reduce network API calls.
pub struct ResolvedAuthenticationClasses {
    resolved_authentication_classes: Vec<core::v1alpha1::AuthenticationClass>,
}

impl ResolvedAuthenticationClasses {
    /// Return the (first) TLS `AuthenticationClass` if available
    pub fn get_tls_authentication_class(&self) -> Option<&core::v1alpha1::AuthenticationClass> {
        self.resolved_authentication_classes.iter().find(|auth| {
            matches!(
                auth.spec.provider,
                core::v1alpha1::AuthenticationClassProvider::Tls(_)
            )
        })
    }

    /// Validates the resolved AuthenticationClasses.
    /// Currently errors out if:
    /// - More than one AuthenticationClass was provided
    /// - AuthenticationClass mechanism was not supported
    pub fn validate(&self) -> Result<Self, Error> {
        if self.resolved_authentication_classes.len() > 1 {
            return Err(Error::MultipleAuthenticationClassesProvided);
        }

        for auth_class in &self.resolved_authentication_classes {
            match &auth_class.spec.provider {
                core::v1alpha1::AuthenticationClassProvider::Tls(_) => {}
                core::v1alpha1::AuthenticationClassProvider::Ldap(_)
                | core::v1alpha1::AuthenticationClassProvider::Oidc(_)
                | core::v1alpha1::AuthenticationClassProvider::Static(_)
                | core::v1alpha1::AuthenticationClassProvider::Kerberos(_) => {
                    return Err(Error::AuthenticationMethodNotSupported {
                        authentication_class: ObjectRef::from_obj(auth_class),
                        method: auth_class.spec.provider.to_string(),
                    });
                }
            }
        }

        Ok(self.clone())
    }

    /// USE ONLY IN TESTS! We can not put it behind `#[cfg(test)]` because of <https://github.com/rust-lang/cargo/issues/8379>
    pub fn new_for_tests() -> Self {
        ResolvedAuthenticationClasses {
            resolved_authentication_classes: vec![],
        }
    }
}

/// Resolve provided AuthenticationClasses via API calls and validate the contents.
/// Currently errors out if:
/// - AuthenticationClass could not be resolved
/// - Validation failed
pub async fn resolve_authentication_classes(
    client: &Client,
    auth_classes: &Vec<v1alpha1::ZookeeperAuthentication>,
) -> Result<ResolvedAuthenticationClasses, Error> {
    let mut resolved_authentication_classes: Vec<core::v1alpha1::AuthenticationClass> = vec![];

    for auth_class in auth_classes {
        resolved_authentication_classes.push(
            core::v1alpha1::AuthenticationClass::resolve(client, &auth_class.authentication_class)
                .await
                .context(AuthenticationClassRetrievalSnafu {
                    authentication_class: ObjectRef::<core::v1alpha1::AuthenticationClass>::new(
                        &auth_class.authentication_class,
                    ),
                })?,
        );
    }

    ResolvedAuthenticationClasses {
        resolved_authentication_classes,
    }
    .validate()
}
