package com.github.kohanyirobert.jdbc.mux.simple

import com.github.kohanyirobert.jdbc.mux.adapter.ConnectionAdapter

import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement

class SimpleMuxConnection extends ConnectionAdapter {

  List<Connection> connections

  SimpleMuxConnection(List<Connection> connections) {
    if (connections.empty) {
      throw new IllegalArgumentException()
    }
    this.connections = connections
  }

  @Override
  Statement createStatement() throws SQLException {
    new SimpleMuxStatement(connections.collect { it.createStatement() })
  }

  @Override
  void rollback() throws SQLException {
    connections.each { it.rollback() }
  }
}
