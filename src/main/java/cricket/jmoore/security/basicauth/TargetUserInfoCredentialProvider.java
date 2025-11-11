/* Licensed under Apache-2.0 */
package cricket.jmoore.security.basicauth;

import io.confluent.kafka.schemaregistry.client.security.basicauth.UserInfoCredentialProvider;

public class TargetUserInfoCredentialProvider extends UserInfoCredentialProvider
{
    @Override
    public String alias() {
        return "TARGET_USER_INFO";
    }
}
