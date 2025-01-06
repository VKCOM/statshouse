import js from '@eslint/js';
import globals from 'globals';
import react from 'eslint-plugin-react';
import reactHooks from 'eslint-plugin-react-hooks';
import reactRefresh from 'eslint-plugin-react-refresh';
import prettier from 'eslint-plugin-prettier';
import tseslint from 'typescript-eslint';

export default tseslint.config({
  ignores: ['src/lib/**'],
  extends: [js.configs.recommended, ...tseslint.configs.recommended],
  files: ['src/**/*.{ts,tsx,js,jsx}'],
  languageOptions: {
    ecmaVersion: 2020,
    globals: globals.browser,
  },
  settings: {
    react: { version: 'detect' },
  },
  plugins: {
    'react-hooks': reactHooks,
    'react-refresh': reactRefresh,
    prettier,
    react,
  },
  rules: {
    ...reactHooks.configs.recommended.rules,
    ...react.configs.recommended.rules,
    ...react.configs['jsx-runtime'].rules,
    'react-refresh/only-export-components': ['warn', { allowConstantExport: true }],
    // 'react-hooks/rules-of-hooks': 'warn',
    'react-hooks/exhaustive-deps': [
      'error',
      {
        enableDangerousAutofixThisMayCauseInfiniteLoops: true,
      },
    ],
    // indent: 'off',
    'prettier/prettier': 'warn',
    'arrow-body-style': ['warn', 'as-needed'],
    //       'prefer-arrow-callback': 'off',
    'react/no-unescaped-entities': 'off',
    'no-console': 'warn',
    'no-empty': ['error', { allowEmptyCatch: true }],
    '@typescript-eslint/no-unused-vars': [
      'error',
      {
        args: 'after-used',
        argsIgnorePattern: '^_',
        caughtErrors: 'all',
        caughtErrorsIgnorePattern: '^_',
        destructuredArrayIgnorePattern: '^_',
        varsIgnorePattern: '^_',
        ignoreRestSiblings: true,
      },
    ],
    '@typescript-eslint/no-explicit-any': ['error', { fixToUnknown: true, ignoreRestArgs: true }],
    'prefer-const': [
      'error',
      {
        destructuring: 'all',
        ignoreReadBeforeAssign: true,
      },
    ],
    '@typescript-eslint/no-empty-object-type': [
      'error',
      {
        allowWithName: 'Props$',
      },
    ],
    '@typescript-eslint/no-import-type-side-effects': 'error',
    'sort-imports': [
      'error',
      {
        ignoreCase: true,
        ignoreDeclarationSort: true,
        ignoreMemberSort: false,
        memberSyntaxSortOrder: ['none', 'all', 'multiple', 'single'],
        allowSeparatedGroups: false,
      },
    ],
  },
});
