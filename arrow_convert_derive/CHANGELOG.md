# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.6.7 (2024-05-01)

### Other

 - <csr-id-566214e43993bb60277d1849383a88f1c4c9bd30/> better no-fields error

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 13 commits contributed to the release over the course of 31 calendar days.
 - 1 commit was understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Merge pull request #6 from aldanor/feature/more-attrs ([`aa60656`](https://github.com/Swoorup/arrow-convert/commit/aa60656a69e6283e49fde75ce502500d4760e409))
    - Initial support for arrow_field(name = "...") ([`fa20bd4`](https://github.com/Swoorup/arrow-convert/commit/fa20bd4056c65c36b258896e9e020cc445c4ff45))
    - Better no-fields error ([`566214e`](https://github.com/Swoorup/arrow-convert/commit/566214e43993bb60277d1849383a88f1c4c9bd30))
    - Migrate to syn 2.0 crate ([`f66efbe`](https://github.com/Swoorup/arrow-convert/commit/f66efbe0cef7630d0ec2a29336ed1f9ff211d412))
    - Remove need for Native type, added array[u8] and string reference serialisation ([`a67d32e`](https://github.com/Swoorup/arrow-convert/commit/a67d32ea8f708d2487941e6d7a933fbd484a3d12))
    - Rework IntoArrowArrayIterator to ArrowArrayIterable using a lending iterator ([`5ed817c`](https://github.com/Swoorup/arrow-convert/commit/5ed817c0c13ec258f8ef074986b30237c2391efc))
    - Fix panic message compilation error in derive_struct ([`4b166b0`](https://github.com/Swoorup/arrow-convert/commit/4b166b0cc49a48338b3cdcd7d67e3dc077fba52a))
    - Unify cargo.toml and bump version ([`8bfdb23`](https://github.com/Swoorup/arrow-convert/commit/8bfdb23e6291aea22b445fe5eb941e3caa25bb87))
    - Merge pull request #2 from Swoorup/sj-migrate-to-arrow ([`37e78ca`](https://github.com/Swoorup/arrow-convert/commit/37e78ca9465de7496f340b3afbee78f5d7b35805))
    - Added support for arrays ([`6ae0e04`](https://github.com/Swoorup/arrow-convert/commit/6ae0e04ca86447f8197f679a67cdf8029a92f798))
    - Fix field accesses serializing when using names like `min` and `max` ([`dd1fa17`](https://github.com/Swoorup/arrow-convert/commit/dd1fa17aff5afb528012769529de95e2ae7502f0))
    - Merge pull request #1 from Swoorup/sj-migrate-to-arrow ([`cc32c2f`](https://github.com/Swoorup/arrow-convert/commit/cc32c2fa21aff22807c1758f87a64c3d0ad61f3a))
    - Ported arrow2-convert to use arrow-rs library. ([`1e4c016`](https://github.com/Swoorup/arrow-convert/commit/1e4c016891f1127ad91dbe0ba445d4b478bd9233))
</details>

