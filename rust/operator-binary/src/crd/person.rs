use serde::{Deserialize, Serialize};
use stackable_operator::{
    kube::CustomResource,
    schemars::{self, JsonSchema},
    versioned::versioned,
};

#[versioned(
    version(name = "v1alpha1"),
    version(name = "v1alpha2"),
    version(name = "v1beta1"),
    version(name = "v2"),
    version(name = "v3")
)]
pub mod versioned {
    #[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[versioned(k8s(
        group = "zookeeper.stackable.tech",
        kind = "Person",
        plural = "persons",
        namespaced,
        crates(
            kube_core = "stackable_operator::kube::core",
            kube_client = "stackable_operator::kube::client",
            k8s_openapi = "stackable_operator::k8s_openapi",
            schemars = "stackable_operator::schemars",
            versioned = "stackable_operator::versioned",
        ),
        options(enable_tracing),
    ))]
    #[serde(rename_all = "camelCase")]
    struct PersonSpec {
        username: String,

        // In v1alpha2 first and last name have been added
        #[versioned(added(since = "v1alpha2"))]
        first_name: String,
        #[versioned(added(since = "v1alpha2"))]
        last_name: String,

        // We started out with a enum. As we *need* to provide a default, we have a Unknown variant.
        // Afterwards we figured let's be more flexible and accept any arbitrary String.
        #[versioned(
            added(since = "v2", default = "default_gender"),
            changed(since = "v3", from_type = "Gender")
        )]
        gender: String,
    }
}
fn default_gender() -> Gender {
    Gender::Unknown
}

#[derive(
    Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, JsonSchema, Serialize,
)]
#[serde(rename_all = "PascalCase")]
pub enum Gender {
    Unknown,
    Male,
    Female,
}

impl From<Gender> for String {
    fn from(value: Gender) -> Self {
        match value {
            Gender::Unknown => "Unknown".to_owned(),
            Gender::Male => "Male".to_owned(),
            Gender::Female => "Female".to_owned(),
        }
    }
}

impl From<String> for Gender {
    fn from(value: String) -> Self {
        match value.as_str() {
            "Male" => Self::Male,
            "Female" => Self::Female,
            _ => Self::Unknown,
        }
    }
}
