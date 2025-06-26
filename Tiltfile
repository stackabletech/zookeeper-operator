# If tilt_options.json exists read it and load the default_registry value from it
settings = read_json('tilt_options.json', default={})
registry = settings.get('default_registry', 'oci.stackable.tech/sandbox')

# Configure default registry either read from config file above, or with default value of "oci.stackable.tech/sandbox"
default_registry(registry)

meta = read_json('nix/meta.json')
operator_name = meta['operator']['name']

custom_build(
    registry + '/' + operator_name,
    'make regenerate-nix && nix-build . -A docker --argstr dockerName "${EXPECTED_REGISTRY}/' + operator_name + '" && ./result/load-image | docker load',
    deps=['rust', 'Cargo.toml', 'Cargo.lock', 'default.nix', "nix", 'build.rs', 'vendor'],
    ignore=['*.~undo-tree~'],
    # ignore=['result*', 'Cargo.nix', 'target', *.yaml],
    outputs_image_ref_to='result/ref',
)

# We need to set the correct image annotation on the operator Deployment to use e.g.
# oci.stackable.tech/sandbox/opa-operator:7y19m3d8clwxlv34v5q2x4p7v536s00g instead of
# oci.stackable.tech/sandbox/opa-operator:0.0.0-dev (which does not exist)
k8s_kind('Deployment', image_json_path='{.spec.template.metadata.annotations.internal\\.stackable\\.tech/image}')

k8s_yaml(helm(
   'deploy/helm/' + operator_name,
   name=operator_name,
   namespace="stackable-operators",
   set=[
      'image.repository=' + registry + '/' + operator_name,
   ],
))
