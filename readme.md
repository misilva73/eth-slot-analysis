# Ethereum slot analysis

An empirical analysis of slot timings on Ethereum


## Analysis status

![timings_diagram](timings_diagram.png)

### Block and blob arrivals

- [ ] Collect larger sample of block arrivals and estimate stats per slot -> this will be used for the attestation model
- [ ] Analyse blob arrivals and propagation (similar to what we did for blocks)

### Attestation arrivals

#### Missed slot attestations

Here, we are focussed on pure attestation propagation time.

- [ ] Collect larger sample of attestations from missing slots
- [ ] Join with data on attestor - entity, country, client (using ethseer or [validator_metadata](https://dune.com/data/dune.rig_ef.validator_metadata)?)
- [ ] Analyze late arrivals (i.e. after 1.5s). Are they overrepresented across any attestor feature?
- [ ] Build regression model to predict attestation arrival and compute feature importance.

#### Non-missed slot attestations

Here, we are focussed on the overall attestation arrivals, which includes bock propagation, block execution and validation and attestation propagation. Besides being a more realistic scenario than missed slots, we have a wider set of factor to account for late arrivals.

- [X] Collect raw sample of attestations from two types of slots:
  - [X] Relay blocks
  - [X] Self-build blocks (still need to check how to get this data)
- [ ] Join with data on attestor - entity, country, client (using ethseer or [validator_metadata](https://dune.com/data/dune.rig_ef.validator_metadata)?)
- [X] Join with data on block - size, gas used, # transactions block propagation time (avg & p95)
- [X] Analyze late arrivals (i.e. after 4.5s). Are they overrepresented across any attestor feature or block feature?
- [ ] Build regression model to predict attestation arrival and compute feature importance.

#### Additional analysis

- [ ] Which validators have significantly different timings between missed slots and normal slots? These are likely the non-prysm validators as all the other clients don't wait for the 4s mark to send their attestations.
- [ ] Exclude blocks with single transactions using more than 16M gas units -> these blocks take longer on average to execute, and we already have an EIP that will fix the transaction size to 16M. 


### Aggregations

WIP
