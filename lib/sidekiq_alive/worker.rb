# frozen_string_literal: true

module SidekiqAlive
  class Worker
    include Sidekiq::Worker
    sidekiq_options retry: false

    def perform(_hostname = SidekiqAlive.hostname)
      # Checks if custom liveness probe passes should fail or return false
      return unless config.custom_liveness_probe.call

      # Remove Zombie Queues
      remove_zombie_queues
      
      # Writes the liveness in Redis
      write_living_probe
      # schedules next living probe
      self.class.perform_in(config.time_to_live / 2, current_hostname)
    end

    def remove_zombie_queues
      queues = Sidekiq::Queue.all

      queues.each do |queue|
        next unless queue.name.starts_with? 'sidekiq_alive-'

        registered_queues = SidekiqAlive.registered_instances.map { |i| "sidekiq_alive-#{i.split('::')[1]}" }

        next if registered_queues.include? queue.name

        Rails.logger.debug "Clearing SidekiqAlive zombine queue #{queue.name}"
        queue.clear
      end
    end
      
    end
    def hostname_registered?(hostname)
      SidekiqAlive.registered_instances.any? do |ri|
        /#{hostname}/ =~ ri
      end
    end

    def write_living_probe
      # Write liveness probe
      SidekiqAlive.store_alive_key
      # Increment ttl for current registered instance
      SidekiqAlive.register_current_instance
      # after callbacks
      begin
        config.callback.call
      rescue StandardError
        nil
      end
    end

    def current_hostname
      SidekiqAlive.hostname
    end

    def config
      SidekiqAlive.config
    end
  end
end
