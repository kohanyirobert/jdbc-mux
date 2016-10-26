package com.github.kohanyirobert.jdbc.mux.simple

import com.github.kohanyirobert.jdbc.mux.exception.MuxSQLException
import spock.lang.Specification

import java.sql.Connection
import java.sql.Statement

class SimpleMuxConnectionSpec extends Specification {

  def 'new mux connection with null fails'() {
    when:
    new SimpleMuxConnection(null)

    then:
    thrown(NullPointerException)
  }

  def 'new mux connection with empty fails'() {
    when:
    new SimpleMuxConnection([])

    then:
    thrown(IllegalArgumentException)
  }

  def 'mux executeUpdate ok if all connections returns the same result'() {
    given:
    def stm1 = Mock(Statement) {
      1 * executeUpdate(_ as String) >> 1
    }
    def stm2 = Mock(Statement) {
      1 * executeUpdate(_ as String) >> 1
    }
    def stm3 = Mock(Statement) {
      1 * executeUpdate(_ as String) >> 1
    }

    and:
    def c1 = Mock(Connection) {
      1 * createStatement() >> stm1
    }
    def c2 = Mock(Connection) {
      1 * createStatement() >> stm2
    }
    def c3 = Mock(Connection) {
      1 * createStatement() >> stm3
    }

    and:
    def c = new SimpleMuxConnection([c1, c2, c3])

    and:
    def stm = c.createStatement()

    when:
    def res = stm.executeUpdate('...')

    then:
    res == 1
  }

  def 'mux executeUpdate fails if not all connections returns the same result'() {
    given:
    def stm1 = Mock(Statement) {
      1 * executeUpdate(_ as String) >> 1
    }
    def stm2 = Mock(Statement) {
      1 * executeUpdate(_ as String) >> 1
    }
    def stm3 = Mock(Statement) {
      1 * executeUpdate(_ as String) >> 0
    }

    and:
    def c1 = Mock(Connection) {
      1 * createStatement() >> stm1
    }
    def c2 = Mock(Connection) {
      1 * createStatement() >> stm2
    }
    def c3 = Mock(Connection) {
      1 * createStatement() >> stm3
    }

    and:
    def c = new SimpleMuxConnection([c1, c2, c3])

    and:
    def stm = c.createStatement()

    when:
    stm.executeUpdate('...')

    then:
    thrown(MuxSQLException)
  }
}
