require 'scheman'

module AlterTableMachine
  class Executor
    def initialize(options)
      raise 'file is required' unless options[:file]
      @file_path = options[:file]
    end

    def execute!
      parsed_schema = parse(load_file)
      sqls = translate(parsed_schema)
      output(sqls)
    end

    def load_file
      File.read(@file_path)
    end

    def parse(text)
      sqls = text.gsub(/[\r\n]/, "").split(";").select {|str|
        str.start_with?("CREATE TABLE")
      }.map {|create_table|
        create_table + ";"
      }
      parser = Scheman::Parsers::Mysql.new
      parser.parse(sqls.join).to_hash
    end

    # charset/collation/row_format
    # and modify columns
    def translate(parsed_schema)
      parsed_schema.map do |schema|
        table_name = schema[:create_table][:name]
        fields = []
        schema[:create_table][:fields].each do |field|
          fields << field[:field] if ['varchar', 'text'].include? field[:field][:type].downcase
        end
        {table_name: table_name, fields: fields}

        sql = "ALTER TABLE `#{table_name}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"
        fields.each do |field|
          col_len = field[:values][0].present? ? "(#{field[:values][0]})" : ""
          sql += ", MODIFY `#{field[:name]}` #{field[:type]}#{col_len} CHARACTER SET utf8mb4"
          field[:qualifiers].each do |q|
            case q[:qualifier][:type]
            when 'collate'
              sql += " COLLATE utf8mb4_general_ci"
            when 'not_null'
              sql += " NOT NULL"
            when 'default'
              if q[:qualifier][:value][:default_value][:type] == 'string'
                sql += " DEFAULT '#{q[:qualifier][:value][:default_value][:value]}'"
              end
            end
          end
        end
        "#{sql}, ROW_FORMAT=DYNAMIC;"
      end
    end

    def output(sqls = [])
      raise 'No sql will change!' unless sqls
      File.open(File.join(Dir.pwd, 'generated.sql'), 'w') do |file|
        sqls.each do |sql|
          file.puts sql
        end
      end
    end
  end
end
