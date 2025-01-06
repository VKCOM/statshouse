/** @returns {Promise<import('jest').Config>} */

const jestConfig = async () => {
  process.env.TZ = 'Europe/Moscow';
  return {
    clearMocks: true,
    coveragePathIgnorePatterns: ['/node_modules/'],
    moduleDirectories: ['node_modules'],
    moduleFileExtensions: ['js', 'mjs', 'cjs', 'jsx', 'ts', 'tsx', 'json', 'node'],
    moduleNameMapper: {
      '^@/(.*)$': '<rootDir>/src/$1',
      '^~(.*)$': '<rootDir>/node_modules/$1',
      // '^.+\\.(css|scss)$': '<rootDir>/src/testMock/styleMock.ts',
    },
    setupFilesAfterEnv: ['./src/setupTests.ts'],
    testEnvironment: 'jsdom',
    transform: {
      '^.+\\.(t|j)sx?$': ['@swc/jest'],
      '^.+\\.(css|scss)$': 'jest-css-modules-transform',
    },
    transformIgnorePatterns: ['/node_modules/'],
  };
};

export default jestConfig;
