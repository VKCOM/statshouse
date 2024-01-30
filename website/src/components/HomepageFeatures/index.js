import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

const FeatureList = [
  {
    title: 'Use in production at big scale',
    Svg: require('@site/static/img/undraw_docusaurus_mountain.svg').default,
    description: (
      <>
        StatsHouse is the main monitoring system of <a href="https://vk.com">vk.com</a>.
        As of December 2022, main StatsHouse cluster is receiving 650 million metrics
        per second from 15000 servers and stores 4 years of data.
      </>
    ),
  },
  {
    title: 'Get high resolution, low latency data',
    Svg: require('@site/static/img/undraw_docusaurus_tree.svg').default,
    description: (
      <>
        With default metric resolution of 1 second and latency of 5 seconds,
        StatsHouse makes it easy to observe things happening in great detail
        immediately as they are happening.
      </>
    ),
  },
  {
    title: 'Store metrics indefinitely',
    Svg: require('@site/static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        StatsHouse automatically downsamples high-resolution data to 1 minute and 1 hour resolutions with automatic TTL.
        High-resolution data is stored for 2 days, minute-resolution data is stored for a month,
        and hour-resolution data is stored indefinitely.
      </>
    ),
  },
];


function Feature({Svg, title, description}) {
  return (
    <div className={clsx('col col--12')}>
      <div className="text--center">

      </div>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="column">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
