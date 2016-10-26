package com.github.kohanyirobert.jdbc.mux.simple

import com.github.kohanyirobert.jdbc.mux.adapter.StatementAdapter
import com.github.kohanyirobert.jdbc.mux.exception.MuxSQLException

import java.sql.SQLException
import java.sql.Statement

class SimpleMuxStatement extends StatementAdapter {

  static <T> T ok(List<T> results) throws SQLException {
    def first = results.first()
    if (results.tail().every { it == first }) {
      return first
    }
    throw new MuxSQLException()
  }

  List<Statement> statements

  SimpleMuxStatement(List<Statement> statements) {
    if (statements.empty) {
      throw new IllegalArgumentException()
    }
    this.statements = statements
  }

  @Override
  int executeUpdate(String sql) throws SQLException {
    ok(statements.collect { it.executeUpdate(sql) })
  }

  @Override
  boolean execute(String sql) throws SQLException {
    ok(statements.collect { it.execute(sql) })
  }
}
