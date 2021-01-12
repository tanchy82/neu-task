package com.oldtan.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.oas.annotations.EnableOpenApi;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 20-12-31
 */
@Configuration
@EnableOpenApi
public class SwaggerConfig {

    @Bean
    public Docket createRestApi() {
        return new Docket(DocumentationType.OAS_30)
                .apiInfo(new ApiInfoBuilder()
                        .title("Restful api documentation for Neu-task project")
                        .description("This documentation is limited to the use of the front and backend development debugging.<br/><br/>" +
                                "If you encounter problems, please contact the development engineer old Tan.")
                        .version("Development debugging version")
                        .build())
                .select()
                .paths(PathSelectors.any())
                .build();
    }

}
