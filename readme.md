# Ethereum slot analysis

An empirical analysis of slot timings on Ethereum


## Analysis status

- [X] Initial EDA on beacon datasets from Xatu
  - We see a significant skew in attestation timings when compared to other analysis. This is likely paused by the nodes being subscribed to all committee and thus not being able to handle the load
- [X] Check whether IHAVE message have a good enough coverage to estimate the attestation CDF for all the peers
  - TLDR: it does not work 😢
- [X] Attestation time (since slot start) for new libp2p dataset (i.e. after the deployment of the forked prysm nodes)
  - Oversubcription make a significant difference, both in the number of attestation per committee observed and the arrival timings
- [X] Analyze attestation timings for missed slots -> we know that in these slots the attestors cannot attest before the 4s mark
- [ ] Which validators have significantly different timings between missed slots and normal slots? These are likely the non-prysm validators as all the other clients don't wait for the 4s mark to send their attestations.
- [ ] Join attestation data with more data sources and analyze. Sources:
  - [ ] Validator metadata from [Thomas](https://dune.com/data/dune.rig_ef.validator_metadata) -> we can segment libp2p peers by country, client and deployment type. Not sure whether we can join this with the attestor ID...
  - [ ] Ultrasound relay data on when the block was delivered to lighthouse -> we could use this as a lower bound for when the block picked by the proposer; this should give us more detailed attestations timings instead of count from the start of the slot.
  - [ ] Titan relay data -> same rationale as the Ultrasound relay data, but I still need to check whether they can share the data with us
- [ ] Join beacon block data with more data sources and analyze. Sources:
  - [ ] Ultrasound relay and Titan relay -> we could use this as a lower bound for when the block picked by the proposer; this should give us more accurate block propagation timings