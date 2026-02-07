package com.streamweaver.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {
    
    @Bean
    public OpenAPI streamWeaverOpenAPI() {
        return new OpenAPI()
            .info(new Info()
                .title("StreamWeaver API")
                .description("A Unified Real-Time Data Fabric for Intelligent Stream Integration, Schema Evolution, and Distributed Analytics")
                .version("v1.0.0")
                .contact(new Contact()
                    .name("StreamWeaver Team")
                    .url("https://github.com/ayoublasfar/StreamWeaver"))
                .license(new License()
                    .name("Apache 2.0")
                    .url("https://www.apache.org/licenses/LICENSE-2.0")));
    }
}
