package com.github.kohanyirobert.jdbc.mux.simple

import com.github.kohanyirobert.jdbc.mux.exception.MuxSQLException
import spock.lang.Specification

import java.sql.Statement

class SimpleMuxStatementSpec extends Specification {

  def 'new mux statement with null fails'() {
    when:
    new SimpleMuxStatement(null)

    then:
    thrown(NullPointerException)
  }

  def 'new mux statement with empty fails'() {
    when:
    new SimpleMuxStatement([])

    then:
    thrown(IllegalArgumentException)
  }

  def 'mux execute ok if all statements return the same result'() {
    given:
    def stm1 = Mock(Statement) {
      1 * execute(_ as String) >> true
    }
    def stm2 = Mock(Statement) {
      1 * execute(_ as String) >> true
    }
    def stm3 = Mock(Statement) {
      1 * execute(_ as String) >> true
    }

    and:
    def stm = new SimpleMuxStatement([stm1, stm2, stm3])

    when:
    def res = stm.execute(_ as String)

    then:
    res == true
  }

  def 'mux execute fails if not all statements return the same result'() {
    given:
    def stm1 = Mock(Statement) {
      1 * execute(_ as String) >> true
    }
    def stm2 = Mock(Statement) {
      1 * execute(_ as String) >> true
    }
    def stm3 = Mock(Statement) {
      1 * execute(_ as String) >> false
    }

    and:
    def stm = new SimpleMuxStatement([stm1, stm2, stm3])

    when:
    stm.execute(_ as String)

    then:
    thrown(MuxSQLException)
  }

  def 'mux executeUpdate ok if all statements return the same result'() {
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
    def stm = new SimpleMuxStatement([stm1, stm2, stm3])

    when:
    def res = stm.executeUpdate(_ as String)

    then:
    res == 1
  }

  def 'mux executeUpdate fails if not all statements return the same result'() {
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
    def stm = new SimpleMuxStatement([stm1, stm2, stm3])

    when:
    stm.executeUpdate(_ as String)

    then:
    thrown(MuxSQLException)
  }
}
