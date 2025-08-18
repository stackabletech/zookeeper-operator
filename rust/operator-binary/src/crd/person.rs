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
    version(name = "v3"),
    options(k8s(experimental_conversion_tracking)),
    crates(
        kube_core = "stackable_operator::kube::core",
        kube_client = "stackable_operator::kube::client",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars",
        versioned = "stackable_operator::versioned",
    )
)]
pub mod versioned {
    #[versioned(crd(group = "test.stackable.tech", status = "PersonStatus",))]
    #[derive(Clone, Debug, CustomResource, Deserialize, JsonSchema, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PersonSpec {
        username: String,

        // In v1alpha2 first and last name have been added
        #[versioned(added(since = "v1alpha2"))]
        first_name: String,

        #[versioned(added(since = "v1alpha2"))]
        last_name: String,

        // We started out with a enum. As we *need* to provide a default, we have a Unknown variant.
        // Afterwards we figured let's be more flexible and accept any arbitrary String.
        #[versioned(added(since = "v2"), changed(since = "v3", from_type = "Gender"))]
        gender: String,

        #[versioned(nested)]
        socials: Socials,
    }

    #[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
    pub struct Socials {
        email: String,

        #[versioned(added(since = "v1beta1"))]
        mastodon: String,
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct PersonStatus {
    pub alive: bool,
}

impl Default for PersonStatus {
    fn default() -> Self {
        Self { alive: true }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "PascalCase")]
pub enum Gender {
    #[default]
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
