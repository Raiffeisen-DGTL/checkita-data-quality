package org.checkita.dqf.connections

import org.scalatest.Tag

/**
 * Tag for integration tests that require a running container runtime (Docker/Podman).
 * These tests are excluded from the default `sbt test` run and can be executed with:
 *   sbt "testOnly * -- -n IntegrationTest"
 */
object IntegrationTestTag extends Tag("IntegrationTest")
