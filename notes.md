# Skylight
What is it?: a mirror of the BlueSky social media network.

Why is this useful to us?:

In the context of accessing data from BlueSky,
facing the obstacle of rate-limited API's and a lack of knowledge of exactly which data we will need to access
I opted for a realtime sync of all available data.
Alternatively, I could have used BlueSky API's on-demand, but would incur rate limits. Ad-hoc caching to alleviate this constraint would amount to an unintentional reinvention of a partial sync.
Accordingly, I could have also mirrored only part of the network, by taking a subset of users and/or fields. In lieu of knowing in advance exactly which users and/or fields will be relevant, we can dodge the question by simply tracking them all. The cost of doing so is low, though not insignificant, as the data cumulatively amounts to less than 1TB.

## Flow

1. Buffer the firehose
2. Backfill
  - List repos (com.atproto.sync.listRepos)
  - Get repo checkpoints (com.atproto.sync.get_repo)
  - Process repos, ignore buffered events earlier than the checkpoint's rev (timestamp)






  # observe new DID's
