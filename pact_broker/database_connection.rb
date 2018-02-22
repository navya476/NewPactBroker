require 'sequel'
require_relative 'database_logger'
require 'uri'

def create_database_connection(logger)
  database_adapter = ENV.fetch('PACT_BROKER_DATABASE_ADAPTER','') != '' ? ENV['PACT_BROKER_DATABASE_ADAPTER'] : 'postgres'
  uri = URI.decode(ENV['DRP_PF_POSTGRES_1'])
  
  ENV['PACT_BROKER_DATABASE_USERNAME']=uri.split(':')[1].split('/')[2]
  ENV['PACT_BROKER_DATABASE_PASSWORD']=uri.split('@')[0].split(':')[2]
  ENV['PACT_BROKER_DATABASE_HOST']=uri.split('@')[1].split(':')[0]
  ENV['PACT_BROKER_DATABASE_NAME']=uri.split('/')[1]
  
  credentials = {
    adapter: database_adapter,
    user: ENV['PACT_BROKER_DATABASE_USERNAME'],
    password: ENV['PACT_BROKER_DATABASE_PASSWORD'],
    host: ENV['PACT_BROKER_DATABASE_HOST'],
    database: ENV['PACT_BROKER_DATABASE_NAME']
  }

  if ENV['PACT_BROKER_DATABASE_PORT'] =~ /^\d+$/
    credentials[:port] = ENV['PACT_BROKER_DATABASE_PORT'].to_i
  end

  ##
  # Sequel by default does not test connections in its connection pool before
  # handing them to a client. To enable connection testing you need to load the
  # "connection_validator" extension like below. The connection validator
  # extension is configurable, by default it only checks connections once per
  # hour:
  #
  # http://sequel.rubyforge.org/rdocplugins/files/lib/sequel/extensions/connection_validator_rb.html
  #
  #
  # A gotcha here is that it is not enough to enable the "connection_validator"
  # extension, we also need to specify that we want to use the threaded connection
  # pool, as noted in the documentation for the extension.
  #
  # 1 means that connections will be validated every time, which avoids errors
  # when databases are restarted and connections are killed.  This has a performance
  # penalty, so consider increasing this timeout if building a frequently accessed service.
  connection = Sequel.connect(credentials.merge(logger: DatabaseLogger.new(logger), encoding: 'utf8'))
  connection.extension(:connection_validator)
  connection.pool.connection_validation_timeout = 1
  connection
end
