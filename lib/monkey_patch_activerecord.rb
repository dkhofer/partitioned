require 'active_record'
require 'active_record/base'
require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/relation.rb'
require 'active_record/persistence.rb'
require 'active_record/relation/query_methods.rb'

#
# Patching {ActiveRecord} to allow specifying the table name as a function of
# attributes.
#
module ActiveRecord
  #
  # Patches for Persistence to allow certain partitioning (that related to the primary key) to work.
  #
  module Persistence
    # This method is patched to provide a relation referencing the partition instead
    # of the parent table.
    def relation_for_destroy

      # ****** BEGIN PARTITIONED PATCH ******
      if self.class.respond_to?(:dynamic_arel_table)
        using_arel_table = dynamic_arel_table()
        self.class.unscoped.from_partition(using_arel_table).where(self.class.primary_key => id)
      else
        # ****** END PARTITIONED PATCH ******

        self.class.unscoped.where(self.class.primary_key => id)

        # ****** BEGIN PARTITIONED PATCH ******
      end
      # ****** END PARTITIONED PATCH ******
    end

    # This method is patched to prefetch the primary key (if necessary) and to ensure
    # that the partitioning attributes are always included (AR will exclude them
    # if the db column's default value is the same as the new record's value).
    def _create_record(attribute_names = @attributes.keys)
      # ****** BEGIN PARTITIONED PATCH ******
      if self.id.nil? && self.class.respond_to?(:prefetch_primary_key?) && self.class.prefetch_primary_key?
        self.id = self.class.connection.next_sequence_value(self.class.sequence_name)
        attribute_names |= ["id"]
      end

      if self.class.respond_to?(:partition_keys)
        attribute_names |= self.class.partition_keys.map(&:to_s)
      end
      # ****** END PARTITIONED PATCH ******

      attributes_values = arel_attributes_with_values_for_create(attribute_names)

      new_id = self.class.unscoped.insert attributes_values
      self.id ||= new_id if self.class.primary_key

      @new_record = false
      id
    end
=begin
    # Updates the associated record with values matching those of the instance attributes.
    # Returns the number of affected rows.
    def _update_record(attribute_names = self.attribute_names)
      
      
      # ****** BEGIN PARTITIONED PATCH ******
      # NOTE(hofer): This patch ensures the columns the table is
      # partitioned on are passed along to the update code so that the
      # update statement runs against a child partition, not the
      # parent table, to help with performance.
      if self.class.respond_to?(:partition_keys)
        attribute_names.concat self.class.partition_keys.map(&:to_s)
        attribute_names.uniq!
      end
      # ****** END PARTITIONED PATCH ******
      
      attributes_values = arel_attributes_with_values_for_update(attribute_names)
      if attributes_values.empty?
        rows_affected = 0
        @_trigger_update_callback = true
      else
        rows_affected = self.class.unscoped._update_record attributes_values, id, id_in_database
        @_trigger_update_callback = rows_affected > 0
      end
      rows_affected
    end
=end
    # Updates the associated record with values matching those of the instance attributes.
    # Returns the number of affected rows.
    def _update_record(attribute_names = self.attribute_names)
      # ****** BEGIN PARTITIONED PATCH ******
      # NOTE(hofer): This patch ensures the columns the table is
      # partitioned on are passed along to the update code so that the
      # update statement runs against a child partition, not the
      # parent table, to help with performance.
      if self.class.respond_to?(:partition_keys)
        attribute_names.concat self.class.partition_keys.map(&:to_s)
        attribute_names.uniq!
      end
      
      attributes_values = arel_attributes_with_values_for_update(attribute_names)
      if attributes_values.empty?
        rows_affected = 0
        @_trigger_update_callback = true
      else
        rows_affected = self.class.unscoped._update_record attributes_values, id, id_was
        @_trigger_update_callback = rows_affected > 0
      end
      rows_affected
    end

  end # module Persistence

  module QueryMethods

    # This method is patched to change the default behavior of select
    # to use the Relation's Arel::Table
    def build_select(arel)
      if !select_values.empty?
        expanded_select = select_values.map do |field|
          columns_hash.key?(field.to_s) ? arel_table[field] : field
        end
        arel.project(*expanded_select)
      else
        # ****** BEGIN PARTITIONED PATCH ******
        # Original line:
        # arel.project(@klass.arel_table[Arel.star])
        arel.project(table[Arel.star])
        # ****** END PARTITIONED PATCH ******
      end
    end

  end # module QueryMethods

  class Relation

    # This method is patched to use a table name that is derived from
    # the attribute values.
    def insert(values)
      primary_key_value = nil

      if primary_key && Hash === values
        primary_key_value = values[values.keys.find { |k|
          k.name == primary_key
        }]

        if !primary_key_value && klass.prefetch_primary_key?
          primary_key_value = klass.next_sequence_value
          values[arel_attribute(klass.primary_key)] = primary_key_value
        end
      end

      im = arel.create_insert

      # ****** BEGIN PARTITIONED PATCH ******
      actual_arel_table = @klass.dynamic_arel_table(Hash[*values.map{|k,v| [k.name,v]}.flatten]) if @klass.respond_to?(:dynamic_arel_table)
      actual_arel_table = @table unless actual_arel_table
      # Original line:
      # im.into @table
      im.into actual_arel_table
      # ****** END PARTITIONED PATCH ******

      substitutes, binds = substitute_values values

      if values.empty? # empty insert
        im.values = Arel.sql(connection.empty_insert_statement_value)
      else
        im.insert substitutes
      end

      @klass.connection.insert(
        im,
        "SQL",
        primary_key || false,
        primary_key_value,
        nil,
        binds)
    end
    
    def _update_record(values, id, id_was) # :nodoc:
      substitutes, binds = substitute_values values

      scope = @klass.unscoped

      if @klass.finder_needs_type_condition?
        scope.unscope!(where: @klass.inheritance_column)
      end

      relation = scope.where(@klass.primary_key => (id_was || id))
      bvs = binds + relation.bound_attributes
      um = relation
        .arel
        .compile_update(substitutes, @klass.primary_key)

      @klass.connection.update(
        um,
        "SQL",
        bvs,
      )
    end

    # NOTE(hofer): This monkeypatch intended for activerecord 4.1.  Based on this code:
    # https://github.com/rails/rails/blob/4-1-stable/activerecord/lib/active_record/relation.rb#L73-L88
    # TODO(hofer): Update this for rails 4.2, looks like the monkeypatched method changes a bit.
    def _update_record(values, id, id_was) # :nodoc:
      substitutes, binds = substitute_values values

      scope = @klass.unscoped

      if @klass.finder_needs_type_condition?
        scope.unscope!(where: @klass.inheritance_column)
      end
      
      relation = scope.where(@klass.primary_key => (id_was || id))
      bvs = binds + relation.bound_attributes
      um = relation
        .arel
        .compile_update(substitutes, @klass.primary_key)

      # ****** BEGIN PARTITIONED PATCH ******
      if @klass.respond_to?(:dynamic_arel_table)
        using_arel_table = @klass.dynamic_arel_table(Hash[*values.map { |k,v| [k.name,v] }.flatten])

        # NOTE(hofer): The um variable got set up using
        # klass.arel_table as its arel value.  So arel_table.name is
        # what gets used to construct the update statement.  Here we
        # set it to the specific partition name for this record so
        # that the update gets run just on that partition, not on the
        # parent one (which can cause performance issues).
        begin
          @klass.arel_table.name = using_arel_table.name
          @klass.connection.update(
            um,
            "SQL",
            bvs,
          )
        ensure
          @klass.arel_table.name = @klass.table_name
        end
      else
        # Original lines:
        @klass.connection.update(
          um,
          "SQL",
          bvs,
        )
      end
      # ****** END PARTITIONED PATCH ******
    end
  end # class Relation
end # module ActiveRecord
