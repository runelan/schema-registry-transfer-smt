/* Licensed under Apache-2.0 */
package cricket.jmoore.security.basicauth;

import io.confluent.kafka.schemaregistry.client.security.basicauth.SaslBasicAuthCredentialProvider;

public class TargetSaslBasicAuthCredentialProvider extends SaslBasicAuthCredentialProvider {
    @Override
    public String alias() {
        return "TARGET_SASL_INHERIT";
    }
}
