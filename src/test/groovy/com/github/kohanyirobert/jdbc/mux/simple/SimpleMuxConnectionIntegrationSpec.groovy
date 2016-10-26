package com.github.kohanyirobert.jdbc.mux.simple

import org.h2.Driver
import spock.lang.Specification

import java.sql.DriverManager
import java.sql.SQLException

class SimpleMuxConnectionIntegrationSpec extends Specification {

  static {
    Class.forName(Driver.name)
  }

  def 'mux execute/rollback ok if not all connections successful'() {
    given:
    def c1 = DriverManager.getConnection('jdbc:h2:mem:')
    def c2 = DriverManager.getConnection('jdbc:h2:mem:')
    def c3 = DriverManager.getConnection('jdbc:h2:mem:')

    and:
    [c1, c2, c3].each {
      it.setAutoCommit(false)
    }

    and:
    [c1, c2].each {
      def s = it.createStatement()
      s.execute('create table a(a int)')
      s.close()
      it.commit()
    }

    and:
    def c = new SimpleMuxConnection([c1, c2, c3])

    and:
    def stm = c.createStatement()

    when:
    stm.execute('insert into a(a) values(1)')

    then:
    thrown(SQLException)

    and:
    [c1, c2].each {
      def s = it.createStatement()
      def rs = s.executeQuery('select count(*) from a')
      rs.next()
      assert rs.getInt(1) == 1
      s.close()
    }

    when:
    stm.close()
    c.rollback()

    then:
    [c1, c2].each {
      def s = it.createStatement()
      def rs = s.executeQuery('select count(*) from a')
      rs.next()
      assert rs.getInt(1) == 0
      s.close()
    }
  }
}
