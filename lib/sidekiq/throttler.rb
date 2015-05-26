require 'sidekiq'
require 'active_support/core_ext/numeric/time'
require 'singleton'

require 'sidekiq/throttler/version'
require 'sidekiq/throttler/rate_limit'

require 'sidekiq/throttler/storage/memory'
require 'sidekiq/throttler/storage/redis'

module Sidekiq
  ##
  # Sidekiq server middleware. Throttles jobs when they exceed limits specified
  # on the worker. Jobs that exceed the limit are requeued with a delay.
  class Throttler
    def initialize(options = {})
      @options = options.dup
    end

    ##
    # Passes the worker, arguments, and queue to {RateLimit} and either yields
    # or requeues the job depending on whether the worker is throttled.
    #
    # @param [Sidekiq::Worker] worker
    #   The worker the job belongs to.
    #
    # @param [Hash] msg
    #   The job message.
    #
    # @param [String] queue
    #   The current queue.
    def call(worker, msg, queue)
      rate_limit = RateLimit.new(worker, msg['args'], queue, @options)

      rate_limit.within_bounds do
        yield
      end

      rate_limit.exceeded do |delay|
        run_in_batch_if_exists msg do
          worker.class.perform_in(delay, *msg['args'])
        end
      end

      rate_limit.execute
    end

    def run_in_batch_if_exists msg
      batch = find_batch(msg)
      if batch
        batch.jobs do
          yield
        end
      else
        yield
      end
    end

    def find_batch msg
      Batch.new msg["bid"] if defined?(Sidekiq::Batch) && msg["bid"]
    end

  end # Throttler
end # Sidekiq
