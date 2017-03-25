require 'spec_helper'
require "#{File.dirname(__FILE__)}/support/tables_spec_helper"

module ActiveRecord::ConnectionAdapters

  describe "PostgreSQLAdapter" do

    let(:check_existence_schema) do
      ActiveRecord::Base.connection.execute <<-SQL
        select oid from pg_catalog.pg_namespace where nspname='employees_partitions';
      SQL
    end

    let(:create_new_schema) do
      ActiveRecord::Base.connection.execute <<-SQL
        create schema employees_partitions;
      SQL
    end

    describe "check next_sequence_value and next_sequence_values methods" do

      include TablesSpecHelper

      before do
        class Employee < ActiveRecord::Base
          include Partitioned::ActiveRecordOverrides
          extend BulkMethodsMixin
        end
        create_tables
      end

      describe "next_sequence_value" do

        it "returns next_sequence_value" do
          expect(ActiveRecord::Base.connection.next_sequence_value(Employee.sequence_name)).to eq 1
          ActiveRecord::Base.connection.execute <<-SQL
            insert into employees(name, company_id) values ('Nikita', 1);
          SQL
          expect(ActiveRecord::Base.connection.next_sequence_value(Employee.sequence_name)).to eq 3
          expect(ActiveRecord::Base.connection.next_sequence_value(Employee.sequence_name)).to eq 4
        end

      end # next_sequence_value

      describe "next_sequence_values" do

        it "returns five next_sequence_values" do
          expect(ActiveRecord::Base.connection.next_sequence_values(Employee.sequence_name, 5)).to eq [1, 2, 3, 4, 5]
        end

      end # next_sequence_values

    end # check next_sequence_value and next_sequence_values methods

    describe "create_schema" do

      context "when call without options" do

        it "created schema" do
          ActiveRecord::Base.connection.create_schema("employees_partitions")
          expect(check_existence_schema.values).not_to be_blank
        end # created schema

      end # when call without options

      context "when call with options unless_exists = true and schema with this name already exist" do

        it "returns nil if schema already exist" do
          create_new_schema
          default_schema = check_existence_schema
          ActiveRecord::Base.connection.create_schema("employees_partitions", :unless_exists => true)
          expect(default_schema.values).to eq check_existence_schema.values
        end # returns nil if schema exist

      end # when call with options unless_exists = true and schema with this name already exist

      context "when call with options unless_exists = false and schema with this name already exist" do

        it "raises ActiveRecord::StatementInvalid" do
          create_new_schema
          expect(lambda {
            ActiveRecord::Base.
                connection.create_schema("employees_partitions", :unless_exists => false)
          }).to raise_error(ActiveRecord::StatementInvalid)
        end # raises ActiveRecord::StatementInvalid

      end # when call with options unless_exists = false and schema with this name already exist

    end # create_schema

    describe "drop_schema" do

      context "when call without options" do

        it "deleted schema" do
          create_new_schema
          ActiveRecord::Base.connection.drop_schema("employees_partitions")
          expect(check_existence_schema.values).to be_blank
        end

      end # when call without options

      context "when call with options if_exist = true and schema with this name don't exist" do

        it "deleted schema" do
          ActiveRecord::Base.connection.drop_schema("employees_partitions", :if_exists => true)
          expect(check_existence_schema.values).to be_blank
        end

      end # when call with options if_exist = true and schema with this name don't exist

      context "when call with options if_exist = false and schema with this name don't exist" do

        it "raises ActiveRecord::StatementInvalid" do
          expect(lambda {
            ActiveRecord::Base.
                connection.drop_schema("employees_partitions", :if_exists => false)
          }).to raise_error(ActiveRecord::StatementInvalid)
        end

      end # when call with options if_exist = false and schema with this name don't exist

      context "when call with option cascade = true" do

        it "deleted schema cascade" do
          create_new_schema
          ActiveRecord::Base.connection.execute <<-SQL
            create table employees_partitions.temp();
          SQL
          ActiveRecord::Base.connection.drop_schema("employees_partitions", :cascade => true)
          expect(check_existence_schema.values).to be_blank
        end

      end # when call with option cascade = true

    end # drop_schema

    describe "add_foreign_key_with_partition" do

      it "added foreign key constraint" do
        create_new_schema
        ActiveRecord::Base.connection.execute <<-SQL
          create table employees_partitions.temp(
            id            serial not null primary key,
            company_id    integer not null
          );
          create table companies(
            id      serial not null primary key
          );
        SQL
        ActiveRecord::Base.connection.add_foreign_key_with_partition("employees_partitions.temp", :company_id, "companies", :id)
        result = ActiveRecord::Base.connection.execute <<-SQL
          SELECT constraint_type FROM information_schema.table_constraints
          WHERE table_name = 'temp' AND constraint_name = 'temp_company_id_fkey';
        SQL
        expect(result.values.first).to eq ["FOREIGN KEY"]
      end

    end # add_foreign_key_with_partition

  end # PostgreSQLAdapter

end # ActiveRecord::ConnectionAdapters
