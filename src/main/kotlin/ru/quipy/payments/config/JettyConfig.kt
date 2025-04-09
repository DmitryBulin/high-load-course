package ru.quipy.payments.config

import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory
import org.springframework.boot.web.embedded.jetty.JettyServerCustomizer
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory
import org.springframework.context.annotation.Bean

@Bean
fun jettyServerCustomizer(): JettyServletWebServerFactory {
    val jettyServletWebServerFactory = JettyServletWebServerFactory()

    val c = JettyServerCustomizer {
        (it.connectors[0].getConnectionFactory("h2c") as HTTP2CServerConnectionFactory).maxConcurrentStreams = 10_000_000
    }

    jettyServletWebServerFactory.serverCustomizers.add(c)
    return jettyServletWebServerFactory
}
