Part of https://github.com/stackabletech/issues/issues/865.

**Problem**: To provision resources inside products operators need to have authenticated and authorized access. Their actions should leave an audit trail inside the products and in SDP component logs. The access has to be explicitly granted by the customer to the SDP component to avoid adding an implicit backdoor into the products.

# Alternatives that were considered

  | Option | Exposure | Disruption | Scales with | Product auth | Identity | Grant | Effort |
  |---|---|---|---|---|---|---|---|
  | **Agent per cluster** | single cluster, pod life | only that agent | clusters | all | per-cluster | explicit, revocable | high |
  | Central proxy | all products, central | all clusters | single proxy | HTTP only | central | central | high |
  | Sidecar | single cluster, local | product restart | product Pods | all | product-local | explicit | medium |
  | Job per reconcile | single cluster, seconds | none | resources | all | per-job | explicit | medium |
  | Directly by operator | all clusters, broad Secret access | operator (all clusters) | single operator | all | shared | implicit | low |

# Proposal: Agent per product cluster

To manage resources within product clusters operators deploy and reconcile agents together with product clusters.
The agent acts as an independent controller that provisions and reconciles internal resources inside of a single product cluster for which it was deployed.

Users have to explicitly enable deployment of the agent in the CR and grant it access with one of the supported mechanisms.
If no agent is deployed or no access is granted internal resources will not be managed by an agent.

Operators retain some responsibility for patching statuses if the responsible agent is not responsive.
This could be by agents regularly renewing heartbeat Leases which the operator watches.

This proposal is modelled after Strimzi's approach with a more explicit access management.
## Authentication

CRDs allow multiple options of providing access to agents based on authentication mechanism supported by the product:
- Managed by the platform: Automatically provisioned by secret-operator (autoTLS, kerberos).
- Managed by the user: Reference to a Secret holding containing static credentials (credentials, certificates, etc.).
- Anonymous: For product clusters in dev environments with no authentication configured

Each type of access has to be explicitly granted to the agent and can be revoked at any time by changing the CR.
Authentication Secrets are never read directly from the k8s API and only mounted into the agent Pods.

## Authorization & Audit
The agent is running as a Deployment in the product cluster's Namespace and uses a ServiceAccount with minimal access to the k8s API.

Authorization of the agent inside the product cluster is managed by the user.
Optionally the operator could allow the user to automatically deploy a ConfigMap with rego rules authorizing the agent.

It's imperative that the agent has a stable and unforgeable identity on which it can be authorized and which will appear in logs of product Pods.

### Required changes for mTLS

Today the certificate subject for autoTLS certificates is always static or the Pod's FQDN. secret-operator would have to be extended to e.g. allow ServiceAccount name as certificate subject.
For products using mTLS like Zookeeper and NiFi SecretClasses for Keystore and Truststore need to be split up to provide more control over authorization.

## CRD Change

```yaml
spec:
  platformAccess:
    enabled: false # no agent is deployed by default
    authentication: # options depend on product
      oidc:
        secret: # static secret containing credentials
      ldap:
        secret: # static secret containing credentials
      tls:
        secretClass: # autoTLS
        secret: # static secret containing TLS cert
      kerberos:
        secretClass: # keytab provided by secret-operator
        secret: # static secret containing kerberos keytab
      anonymous: {} # indicates to the agent that the product cluster doesn't require authentication
    authorization:
      opa:
        authorizePlatform: false # deploys a ConfigMap with rego rules authorizing the agent
```

## Agent binary
Each agent is a separate binary from the operator but lives in the same repository, shares common code with the operator and utilizes operator-rs.
Ideally everything agents do can be implemented in Rust, though for some actions they might need extra binaries in their container or a sidecar.

## Security
Agents have a low attack surface and blast radius.
- Agents expose no interfaces, all communication except to the product cluster is indirect through the k8s API.
- Agents only have access to a single product cluster.
- Agents require minimal permissions in k8s: Watching resources and patching their status.
- Access can be revoked at any time.

## Credential vending
Agents don't require a vending mechanism on secret-operator to access product clusters as access is granted once during agent deployment.

If an agent needs to reconcile an internal resource that references a Secret (e.g. a database connection) the agent has to access the Secret's content during runtime. 
secret-operator could be extended with a runtime API that provides short-lived access to an specific subset of Secrets based on the agent's identity and labels/annotations on Secrets.

Probably out-of-scope: Since this type of internal resources usually can't be read back, the agent could store a hash of the last applied secret value to spot secret rotations.

## Cockpit
Cockpit interacts with internal resources only through the k8s API as the agents don't expose any interface.
