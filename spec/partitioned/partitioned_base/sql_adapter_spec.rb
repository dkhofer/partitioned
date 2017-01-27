require 'spec_helper'

module Partitioned
  class PartitionedBase
    describe SqlAdapter do

      before(:all) do
        class SqlAdapter
          def last_n_partitions_order_by_clause
            return configurator.last_partitions_order_by_clause
          end
        end
        module Sql
          class Employee < PartitionedBase
            def self.partition_table_size
              return 2
            end

            def self.partition_integer_field
              return :id
            end

            partitioned do |partition|
              partition.foreign_key :company_id
              partition.index :id, :unique => true
              partition.check_constraint lambda { |model, id|
                value = model.partition_normalize_key_value(id)
                if model.partition_table_size == 1
                  return "( #{model.partition_integer_field} = #{value} )"
                else
                  return "( #{model.partition_integer_field} >= #{value} and #{model.partition_integer_field} < #{value + model.partition_table_size} )"
                end
              }
            end
          end
        end # Sql
        ActiveRecord::Base.connection.execute <<-SQL
          create table employees
          (
              id               serial not null primary key,
              name             text not null,
              company_id       integer not null
          );
        SQL
      end

      after(:all) do
        ActiveRecord::Base.connection.execute <<-SQL
          drop table employees;
        SQL
        Partitioned::PartitionedBase::Sql.send(:remove_const, :Employee)
      end

      let(:sql_adapter) { Sql::Employee::SqlAdapter.new( Sql::Employee) }

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

      let(:check_existence_table) do
        ActiveRecord::Base.connection.execute <<-SQL
          SELECT relname
          FROM pg_class
          WHERE relname !~ '^(pg_|sql_)'
          AND relkind = 'r';
        SQL
      end

      let(:create_new_partition_table) do
        ActiveRecord::Base.connection.execute <<-SQL
          create table employees_partitions.p1(
            id    serial not null primary key
          );
        SQL
      end

      describe "ensure_always_fail_on_insert_exists" do
        it "generates the db function" do
          sql_adapter.ensure_always_fail_on_insert_exists
          result = ActiveRecord::Base.connection.execute <<-SQL
            SELECT routine_name FROM information_schema.routines
            WHERE specific_schema NOT IN ('pg_catalog', 'information_schema')
            AND type_udt_name != 'trigger';
          SQL
          expect(result.values.first).to eq ["always_fail_on_insert"]
        end
      end # ensure_always_fail_on_insert_exists

      describe "create_partition_schema" do
        it "created schema" do
          sql_adapter.create_partition_schema
          expect(check_existence_schema.values).not_to be_blank
        end
      end # create_partition_schema

      describe "partition_exists?" do

        context "when partition table don't exist" do
          it "returns false" do
            expect(sql_adapter.partition_exists?(1)).to be false
          end
        end # when partition table don't exist

        context "when partition table exist" do
          it "returns true" do
            create_new_schema
            create_new_partition_table
            expect(sql_adapter.partition_exists?(1)).to be true
          end
        end # when partition table exist
      end # partition_exists?

      describe "last_n_partition_names" do

        context "when partition table don't exist" do
          it "returns empty array" do
            expect(sql_adapter.last_n_partition_names).to be_empty
          end
        end # when partition table don't exist

        context "when partition table exist" do
          it "returns partition table name" do
            create_new_schema
            create_new_partition_table
            expect(sql_adapter.last_n_partition_names).to eq ["p1"]
          end
        end # when partition table exist
      end

      describe "add_parent_table_rules" do
        context "when try to insert row into table with rules" do
          it "raises ActiveRecord::StatementInvalid" do
            sql_adapter.add_parent_table_rules
            expect(lambda { ActiveRecord::Base.connection.execute <<-SQL
                insert into employee (name) values ('name');
              SQL
            }).to raise_error(ActiveRecord::StatementInvalid)
          end
        end # when try to insert row into table with rules
      end # add_parent_table_rules

      describe "create_partition_table" do
        it "created partition table" do
          create_new_schema
          expect(lambda {
            sql_adapter.create_partition_table(1)
          }).not_to raise_error
          expect(check_existence_table.values.sort).to match_array [["employees"], ["p1"], ["schema_migrations"]]
        end
      end # create_partition_table

      describe "drop_partition_table" do
        it "deleted partition table" do
          create_new_schema
          sql_adapter.create_partition_table(1)
          sql_adapter.drop_partition_table(1)
          expect(check_existence_table.values).to match_array [["employees"], ["schema_migrations"]]
        end
      end # drop_partition_table

      describe "add_partition_table_index" do
        it "added index for partition table" do
          create_new_schema
          sql_adapter.create_partition_table(1)
          sql_adapter.add_partition_table_index(1)
          result = ActiveRecord::Base.connection.execute <<-SQL
            SELECT count(*) FROM pg_class
            where relname = 'p1_id_udx'
          SQL
          expect(result.values).to eq [[1]]
        end
      end # add_partition_table_index

      describe "add_references_to_partition_table" do
        it "added foreign key constraint" do
          create_new_schema
          sql_adapter.create_partition_table(1)
          ActiveRecord::Base.connection.execute <<-SQL
            create table companies
            (
                id               serial not null primary key,
                name             text null
            );
          SQL
          sql_adapter.add_references_to_partition_table(1)
          result = ActiveRecord::Base.connection.execute <<-SQL
            SELECT constraint_type FROM information_schema.table_constraints
            WHERE table_name = 'p1' AND constraint_name = 'p1_company_id_fkey';
          SQL
          expect(result.values.first).to eq ["FOREIGN KEY"]
        end
      end # add_references_to_partition_table

    end # SqlAdapter
  end # PartitionedBase
end # Partitioned
