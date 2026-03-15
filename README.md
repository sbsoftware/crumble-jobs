# crumble-jobs

Background job framework for Crumble applications.

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     crumble-jobs:
       github: sbsoftware/crumble-jobs
   ```

2. Run `shards install`.

## Usage

```crystal
require "crumble-jobs"
```

### Configure the queue

Configure a single queue backend for the app using the macro. The queue is reused by jobs and workers.

```crystal
Crumble::Jobs.configure_queue Crumble::Jobs::InMemoryQueue.new
# or
Crumble::Jobs.configure_queue Crumble::Jobs::FileQueue.new("./tmp/jobs")
```

After configuration, jobs enqueue through `Crumble::Jobs.queue` and workers default to the same queue.

### Define jobs

```crystal
class IncrementCounterJob < Crumble::Jobs::Job
  params session_key : String, counter_id : Int64

  def perform : Nil
    # ...
  end
end
```

Throttle a job class:

```crystal
class SyncWebhookJob < Crumble::Jobs::Job
  throttle max_jobs: 10, timespan: 5.minutes
  params endpoint : String

  def perform : Nil
    # ...
  end
end
```

`throttle` applies when a worker starts jobs, not when jobs are enqueued.
Enqueueing is never blocked by throttling.
A worker starts at most `max_jobs` jobs of that class inside one window, and the window starts with the first execution in the current burst.
After the limit is reached, later jobs of the same class wait until the window expires, and same-class execution order is preserved.

Enqueue a job:

```crystal
IncrementCounterJob.enqueue("session-uuid", 123)
```

### Worker

```crystal
worker = Crumble::Jobs::Worker.new(max_concurrency: 4)
worker.start
```

## Development

- Format: `crystal tool format`
- Tests: `crystal spec`

## Contributing

1. Fork it (<https://github.com/sbsoftware/crumble-jobs/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Stefan Bilharz](https://github.com/sbsoftware) - creator and maintainer
