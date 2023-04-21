/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-jdbc:2.7.+")
    implementation("org.postgresql:postgresql:42.5.4")
    implementation("mysql:mysql-connector-java:8.0.31")
    implementation("software.amazon.awssdk:rds:2.17.289")
    implementation("software.amazon.awssdk:secretsmanager:2.17.285")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.14.2")
    implementation(project(":aws-advanced-jdbc-wrapper"))
}
