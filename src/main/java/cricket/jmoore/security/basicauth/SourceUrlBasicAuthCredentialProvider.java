/* Licensed under Apache-2.0 */
package cricket.jmoore.security.basicauth;

import io.confluent.kafka.schemaregistry.client.security.basicauth.UrlBasicAuthCredentialProvider;

public class SourceUrlBasicAuthCredentialProvider extends UrlBasicAuthCredentialProvider {
    @Override
    public String alias() {
        return "SOURCE_URL";
    }
}
