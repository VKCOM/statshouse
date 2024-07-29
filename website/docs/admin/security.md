---
sidebar_position: 5
title: Ensure security
---

import Aes from '../img/aes.png'
import NoProxyEncryption from '../img/no-proxy-encryption.png'
import WithProxyEncryption from '../img/with-proxy-encryption.png'

# Ensure security

StatsHouse encrypts data between the agent and the aggregator even if they are located in the same
data center. The AES encryption is used by default.

<img src={Aes} width="700"/>

If your cluster receives data from the agents that live outside the protected perimeter (i.e., outside the data center),
use the ingress proxies. The ingress proxy has a separate set of encryption keys for the external connections.
Read more about the StatsHouse [ingress proxy](overview/components.md#ingress-proxy) component
in the conceptual overview.

## Without the ingress proxy

Check the flow for encrypting data between the agent and the aggregator without the ingress proxy.

<img src={NoProxyEncryption} width="300"/>

The agents send encrypted data to aggregators.
They use the key from the `--aes-pwd-file` directory.

The aggregators decrypt the incoming data.
They use the same key from the `--aes-pwd-file` directory.

## With the ingress proxy

Check the flow for encrypting data between the agent and the aggregator with the ingress proxy in the middle.

<img src={WithProxyEncryption} width="600"/>

The agents send encrypted data to the ingress proxies.
Each agent gets one of the keys from the ingress proxy's `-ingress-pwd-dir` directory as the `-aes-pwd-file` parameter.

The ingress proxies decrypt the incoming data using the keys from the `--ingress-pwd-dir` directory.

The ingress proxies send encrypted data to the aggregators.
They use the key from the `--aes-pwd-file` directory to encrypt the outgoing traffic.

The aggregator starts with the parameter `--aes-pwd-file` to decrypt the incoming traffic.
