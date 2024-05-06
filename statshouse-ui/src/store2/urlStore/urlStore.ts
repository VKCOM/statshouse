import type { Location } from 'history';

import { createStore } from '../createStore';
import { appHistory } from '../../common/appHistory';
import { QueryParams } from './queryParams';
import { arrToObj, getDefaultParams, toTreeObj, TreeParamsObject, treeParamsObjectValueSymbol } from './lib';
import { urlDecode } from './urlDecode';
import { apiDashboardFetch } from '../../api/dashboard';
import { GET_PARAMS } from '../../api/enum';
import { produce } from 'immer';
import { isArray, isObject, mergeLeft } from '../../common/helpers';
import { debug } from '../../common/debug';
import { urlEncode } from './urlEncode';

export type UrlStore = {
  params: QueryParams;
  saveParams: QueryParams;
};

let lastSearch: string = appHistory.location.search;

const isValid: string[] = ['/2/view', '/2/embed'];
export function validPath(location: Location) {
  return isValid.indexOf(location.pathname) > -1;
}

export const useUrlStore = createStore<UrlStore>((setState, getState, store) => {
  let prevLocation = appHistory.location;
  appHistory.listen(({ location, action }) => {
    if (prevLocation.search !== location.search || prevLocation.pathname !== location.pathname) {
      prevLocation = location;
      if (validPath(prevLocation) && lastSearch !== prevLocation.search) {
        lastSearch = prevLocation.search;
        getUrlState(getState().saveParams, prevLocation).then((res) => {
          setState((s) => mergeLeft(s, { ...res }));
        });
      }
    }
  });
  store.subscribe((state, prevState) => {});
  const saveParams = getDefaultParams();
  lastSearch = prevLocation.search;
  // setState({ loading: true });
  getUrlState(saveParams, prevLocation).then((res) => {
    setState((s) => mergeLeft(s, { ...res }));
  });
  return {
    params: saveParams,
    saveParams: saveParams,
  };
}, 'useUrlStore');

export async function getUrlState(
  prevParam: QueryParams,
  location: Location
): Promise<Pick<UrlStore, 'params' | 'saveParams'>> {
  const urlSearchArray = [...new URLSearchParams(location.search)];
  const urlObject = arrToObj(urlSearchArray);
  const urlTree = toTreeObj(urlObject);
  const saveParams = await loadDashboard(prevParam, urlTree, getDefaultParams());
  const params = urlDecode(urlTree, saveParams);
  return {
    params,
    saveParams,
  };
}

export function setUrlStore(next: (draft: UrlStore) => UrlStore | void) {
  const nextState = produce(useUrlStore.getState(), next);
  const search = getUrl(nextState);
  appHistory.push({ search });
  // useUrlStore.setState(nextState);
}

export function getUrl(state: UrlStore): string {
  const urlSearchArray = urlEncode(state.params, state.saveParams);
  return new URLSearchParams(urlSearchArray).toString();
}

export function getDashboardId(urlTree: TreeParamsObject) {
  return urlTree[GET_PARAMS.dashboardID]?.[treeParamsObjectValueSymbol]?.[0];
}

export async function loadDashboard(
  prevParam: QueryParams,
  urlTree: TreeParamsObject,
  defaultParams = getDefaultParams()
) {
  const dashboardId = getDashboardId(urlTree);

  let dashboardParams = defaultParams;
  if (dashboardId) {
    if (dashboardId && prevParam.dashboardId === dashboardId) {
      return prevParam;
    }
    const { response, error } = await apiDashboardFetch({ [GET_PARAMS.dashboardID]: dashboardId });
    if (error) {
      debug.error(error);
    }
    if (response) {
      dashboardParams = normalizeDashboard(response.data?.dashboard?.data, {
        ...defaultParams,
        dashboardId: response.data.dashboard.dashboard_id.toString(),
        dashboardName: response.data.dashboard.name,
        dashboardDescription: response.data.dashboard.description,
        dashboardVersion: response.data.dashboard.version,
      });
    }
  }
  return dashboardParams;
}

export function normalizeDashboard(data: unknown, defaultParams: QueryParams): QueryParams {
  if (isObject(data) && isUrlSearchArray(data.searchParams)) {
    return urlDecode(toTreeObj(arrToObj(data.searchParams)), defaultParams);
  }
  return defaultParams;
}

export function isUrlSearchArray(item: unknown): item is [string, string][] {
  if (isArray(item) && item.every((v) => isArray(v) && typeof v[0] === 'string' && typeof v[1] === 'string')) {
    return true;
  }
  return false;
}

/*
function pTest<T>(callback: (payload: T) => void, payload: T, loop = 100) {
  console.time(callback.name);
  let r;
  for (let i = 0; i < loop; i++) {
    r = callback.call(null, payload);
  }
  console.timeEnd(callback.name);
  return r;
}
const s =
  '?tn=-2&g0.t=Основные%20показатели&g0.n=22&g0.s=3&g1.t=Фичи%20VK%20ID&g1.n=21&g1.s=3&g2.t=Рестор&g2.n=8&g2.s=3&g3.t=Движки&g3.n=7&g3.s=3&g4.t=Капча&g4.n=3&g4.s=3&g5.t=Время%20ответа%20контроллеров%2099pct%20в%20ms&g5.n=6&g5.s=3&g6.t=Ошибки%20контроллеров&g6.n=5&g6.s=3&g7.t=CUA%20%28Check%20User%20Action%29&g7.n=3&g7.s=3&g8.t=Личный%20кабинет&g8.n=14&g8.s=3&g9.t=VK%20Pay&g9.n=1&g9.s=3&g10.t=Core%20Auth&g10.n=17&g10.s=3&g11.t=OAuth&g11.n=12&g11.s=3&g2.v=0&g5.v=0&g7.v=0&g9.v=0&t=0&f=-86400&s=user_registered&cn=R_WD_Регистрации%20%28user_registered%29&qw=count&g=300&qf=0-production&qf=0-staging&t1.s=connect_watcher_user_auth_events&t1.cn=AA_WD_Авторизации%20%28logged_in%29&t1.qw=count&t1.g=300&t1.qf=2-logged_in&t2.s=auth_login_with_password&t2.cn=AA_WD_Парольные%20авторизации&t2.qw=count&t2.g=300&t2.qf=0-production&t2.qf=0-staging&t3.s=auth_login_with_password&t3.cn=AA_WD_Парольные%20авторизации%20%28oauth%29&t3.qw=count&t3.g=300&t3.qf=1-oauth&t4.s=authcheck_pass&t4.cn=AT_WD_Подтверждение%20фактора%20владения%20%28authcheck_pass%29&t4.qw=count&t4.g=300&t4.qb=3&t5.s=auth_login_without_password&t5.cn=AA_WD_Беспарольные%20авторизации&t5.qw=count&t5.g=300&t5.qb=2&t6.s=authcheck_pass&t6.cn=AT_WD_Подтверждение%20фактора%20владения%20бесшовно%20%28authcheck_pass%29&t6.qw=count&t6.g=300&t6.qb=4&t6.qf=3-trusted_hash&t6.qf=4-android&t6.qf=4-iphone&t7.s=silent_auth&t7.cn=AA_WD_Бесшовные%20авторизации%20%28silent_auth%29&t7.qw=count&t7.g=300&t7.mh=1&t8.s=auth_fingerprints_usage&t8.cn=AT_WD_Использование%20fingerprint%20при%20бесшовной%20авторизации%20IOS&t8.qw=count&t8.g=300&t9.s=silent_auth&t9.cn=AU_WD_Получение%20silent%20токена%20при%20авторизации&t9.qw=count&t9.g=300&t9.qf=1-get_credentials&t10.s=oauth_errors&t10.cn=AA_Ошибки%20в%20актах%20на%20домене%20oauth.vk.com&t10.qw=count&t10.g=300&t11.s=api_errors&t11.cn=AA_Ошибки%20сервера%20api-секция%20Auth%20&t11.qw=count&t11.g=900&t11.qf=1-auth&t11.qf=3-ERR_API_10&t12.s=api_methods&t12.cn=R_Количество%20вызовов%20api-метода%20регистрации&t12.qw=count&t12.g=300&t12.qf=2-signup&t13.s=mrg_smsapi_calls&t13.cn=C_WD_Количество%20обращений%20к%20SMSAPI&t13.qw=count&t13.g=300&t13.qb=3&t13.n=10&t14.s=connect_logout&t14.cn=С_WD_Количество%20разлогинов&t14.qw=count&t14.g=300&t14.qb=2&t15.s=connect_logout&t15.cn=C_Технические%20разлогины%20моб.%20клиенты&t15.qw=count&t15.g=900&t15.qb=6&t15.qb=8&t15.qf=2-full_logout&t15.qf=8~multiaccount_logout&t15.qf=8~user&t16.s=connect_session_reader_factory&t16.cn=AT_Количество%20ошибок%20при%20парсинге%20сессий.&t16.qw=count&t16.g=300&t16.qb=1&t16.qf=1-parse&t16.qf=2~ok&t17.s=apps_api_errors&t17.cn=AT_Количество%20ошибок%20авторизации%20в%20API%20для%20IOS%2FAndroid&t17.g=300&t17.qf=1-%202274003&t17.qf=1-%203140623&t17.qf=3-ERR_API_5&t18.s=api_errors&t18.cn=AT_Количество%20ошибок%20для%20api-метода%20проверки%20второго%20фактора%20или%20телеофна.&t18.qw=count&t18.g=300&t18.qb=3&t18.qf=2-auth.validatePhone&t19.s=apps_api&t19.cn=C_Обмен%20токена%20по%20приложениям%20&t19.qw=count&t19.g=300&t19.qb=1&t19.qf=3-exchangeSilentAuthToken&t19.n=10&t20.s=connect_password_check&t20.cn=AA_Проверки%20пароля%20в%20разбивке%20успешная%20%2F%20неуспешная&t20.qw=count&t20.g=900&t20.qb=1&t21.s=connect_password_check&t21.cn=AA_Ошибки%20движка%20при%20проверки%20пароля&t21.qw=count&t21.g=900&t21.qf=2-engine_fail&t22.s=connect_watcher_user_auth_events&t22.cn=AA_Запись%20продуктового%20события%20UserAuthEvent%20в%20CH&t22.qw=count&t22.g=300&t23.s=oauth_generate_token_vkui_internal&t23.cn=AU_WD_Генерация%20токена%20для%20VKUI%20Internal%20приложения&t23.g=300&t23.qf=1-7344294&t23.qf=2-auth_by_token&t23.qf=3-access_token&t24.s=code_authorization_events&t24.cn=AA_Прохождение%20по%20code%20flow%20в%20старом%20oauth&t24.qw=count&t24.g=300&t24.qb=10&t24.qf=1-processAuthCode&t24.qf=10-0&t24.qf=10-1&t25.s=code_authorization_events&t25.cn=AA_Количество%20авторизаций%20по%20QR%20коду&t25.qw=count&t25.g=300&t25.qb=10&t26.s=passwordless_auth_events&t26.cn=AA_WD_Обмен%20%28Расширение%29%20partial-токенов%20на%20access%20token&t26.qw=count&t26.g=300&t26.qf=1-partial_extended&t27.s=profile_extended&t27.cn=R_Ручная%20дорегистрация%20пользователей%20&t27.qw=count&t27.g=300&t27.qf=2-manual&t28.s=connect_event_sender&t28.cn=AA_WD_Callback%20API%3A%20количество%20доставок%20событий%20сервисам&t28.qw=count&t28.g=300&t29.s=connect_event_publisher&t29.cn=C_WD_События%20шины%20данных%20Connect&t29.qw=count&t29.g=300&t30.s=auth_login_phone_valid_confirm&t30.cn=AT_WD_Успешные%20валидации%20номера%20при%20авторизации&t30.qw=count&t30.g=300&t31.s=auth_phone_valid_check&t31.cn=AA_WD_Успешные%20валидации%20номера%20по%20запросу%20auth.validatePhoneCheck&t31.qw=count&t31.g=300&t32.s=oauth_stats_authorize_as_vkid&t32.cn=AA_WD_Авторизация%20через%20мигрированный%20флоу%20VKID&t32.qw=count&t32.g=300&t32.qb=1&t32.qf=1-code_generated&t32.qf=1-oauth_authorize&t32.qf=1-token_generated&t33.s=oauth_stats_authorize_as_vkid&t33.cn=AA_WD_Ошибки%20авторизация%20через%20мигрированный%20флоу%20VKID&t33.qw=count&t33.g=300&t33.qf=1~code_generated&t33.qf=1~oauth_authorize&t33.qf=1~oauth_authorize_sat&t33.qf=1~token_generated&t34.s=oauth_stats_authorize_as_vkid&t34.cn=AA_WD_Успешные%20авторизации%20в%20мигрированном%20флоу%20через%20SAT&t34.qw=count&t34.g=300&t34.qf=1-oauth_authorize_sat&t35.s=silent_auth&t35.cn=AA_Авторизации%20ЭЖД&t35.qw=count&t35.g=300&t35.qf=1-get_credentials_web&t35.qf=2-app51437259&t35.qf=2-app51458637&t35.qf=2-app51464477&t35.qf=2-app51464551&t35.qf=2-app51472774&t35.qf=2-app51482366&t35.qf=2-app51482379&t35.qf=2-app51483644&t35.qf=2-app51489771&t35.qf=2-app51490122&t35.qf=2-app51492764&t35.qf=2-app51493246&t35.qf=2-app51496449&t35.qf=2-app51497976&t35.qf=2-app51503941&t35.qf=2-app51534392&t35.qf=2-app51536536&t35.qf=2-app6146827&t35.qf=2-app6482950&t35.qf=2-app7556576&t35.qf=2-app8080585&t35.qf=2-app8206423&t35.qf=2-app8206717&t35.qf=2-app8223270&t36.s=saved_users&t36.cn=AT_WD_Успешное%20прохождение%20сохраненного%20входа&t36.qw=count&t36.g=300&t36.qf=1-success&t37.s=multi_account&t37.cn=AA_Использование%20мультиаккаунта&t37.qw=count&t37.g=300&t37.qf=1~error&t38.s=multi_account&t38.cn=AA_WD_Ошибки%20мультиаккаунта%20на%20Web&t38.qw=count&t38.g=300&t38.qf=0-production&t38.qf=1-error&t39.s=api_errors&t39.cn=AA_WD_Ошибки%20рефреша%20токенов%20в%20мультиаккаунте&t39.qw=count&t39.g=300&t39.qf=0-production&t39.qf=1-auth&t39.qf=2-auth.refreshTokens&t40.s=connect_passkey&t40.cn=AA_Успешные%20входы%20по%20OnePass&t40.qw=count&t40.qf=1-login&t40.qf=2-finish&t40.qf=3-success&t41.s=connect_passkey&t41.cn=AA_Инициация%20входа%20по%20OnePass&t41.qw=count&t41.qf=1-login&t41.qf=2-begin&t41.qf=3-success&t42.s=connect_passkey&t42.cn=AA_Успешные%20регистрации%20ключей%20OnePass&t42.qw=count&t42.qf=1-register&t42.qf=2-finish&t42.qf=3-success&t43.s=restore2_api_result&t43.cn=RR_WD_Вызов%20рестор%20API&t43.qw=count&t43.g=300&t43.qb=1&t44.s=restore_forgot_pass&t44.cn=RR_WD_Смена%20пароля%20через%20почту&t44.qw=count&t44.g=300&t44.qf=1-email_pass_changed&t45.s=restore_forgot_pass&t45.cn=RR_WD_Смена%20пароля%20через%20телефон&t45.qw=count&t45.g=300&t45.qf=1-phone_pass_changed&t46.s=restore2_requests&t46.cn=RR_WD_Заявки%20в%20доверенный%20рестор&t46.qw=count&t46.g=300&t46.qf=1-trusted_friends&t47.s=restore2_requests&t47.cn=RR_WD_Заявки%20в%20упрощенный%20рестор&t47.qw=count&t47.g=300&t47.qf=1-with_password&t48.s=restore2_requests&t48.cn=RR_WD_Заявки%20в%20расширенный%20рестор&t48.qw=count&t48.g=300&t48.qf=1-extended&t49.s=restore_forgot_pass&t49.cn=RR_Генерация%20внутренних%20токенов&t49.qw=count&t49.g=300&t49.qf=1-token_generated&t50.s=api_errors&t50.cn=RR_Ошибки%20API&t50.qw=count&t50.g=300&t50.qb=2&t50.qf=1-restore&t50.qf=2-restore.authByCode&t50.qf=2-restore.checkInfo&t50.qf=2-restore.checkPasswordComplexity&t50.qf=2-restore.checkPhoneNumber&t50.qf=2-restore.createFullRequest&t50.qf=2-restore.createSimpleRequest&t50.qf=2-restore.createTrustedRequest&t50.qf=2-restore.disablePageCreateRequest&t50.qf=2-restore.findUser&t50.qf=2-restore.init&t50.qf=2-restore.resetPassword&t50.qf=2-restore.resetPasswordByPhone&t50.qf=2-restore.resetSessions&t51.s=engines_rpc_fails&t51.cn=AU_Ошибки%20движка%20паролей&t51.qw=count&t51.g=300&t51.qf=1-user_pwd&t52.s=engines_rpc_fails&t52.cn=AT_Ошибки%20движка%20поиска%20по%20ID&t52.qw=count&t52.g=300&t52.qf=1-id_search&t53.s=engines_rpc_fails&t53.cn=R_Ошибки%20к%20кластеру%20register%20%28MC%20для%20реги%29&t53.qw=count&t53.g=300&t53.qf=1-register&t54.s=engines_rpc_fails&t54.cn=AA_Ошибки%20к%20движку%20принятых%20скопов%20пользователя%20%28members_apps%29&t54.qw=count&t54.g=300&t54.qf=1-member_apps&t55.s=engines_rpc_fails&t55.cn=AA_Ошибки%20к%20движку%20рейтинга%20для%20показа%20капчи&t55.qw=count&t55.g=300&t55.qf=1-rating_ip_address&t56.s=engines_rpc_fails&t56.cn=AA_Ошибки%20к%20общему%20MC%2C%20PMC&t56.qw=count&t56.g=300&t56.qf=1-mc&t56.qf=1-pmc&t57.s=engines_rpc_fails&t57.cn=AA_Ошибки%20к%20движку%20хранящих%20параметры%20авторизационнх%20аппок&t57.qw=count&t57.g=300&t57.qf=1-connect_apps&t58.s=site_captcha&t58.cn=C_Количество%20впервые%20показанных%20капч&t58.qw=count&t58.g=300&t58.qb=2&t58.qf=2-api&t58.qf=2-mvk&t58.qf=2-web&t58.qf=3-first_show&t58.qf=4-difficult&t58.qf=4-simple&t59.s=site_captcha&t59.cn=С_Количество%20показанных%20капч%20при%20авторизации%20и%20регистрации%20и%20ресторе&t59.qw=count&t59.g=300&t59.qb=2&t59.qf=1-api-oauth&t59.qf=1-api_auth_check_phone_code_flood&t59.qf=1-api_auth_code_auth&t59.qf=1-api_auth_signup&t59.qf=1-api_auth_signup_2&t59.qf=1-api_auth_signup_3&t59.qf=1-api_auth_signup_sms&t59.qf=1-api_auth_signup_strong&t59.qf=1-api_auth_validate_account&t59.qf=1-api_auth_validate_account_ip&t59.qf=1-api_auth_validate_phone&t59.qf=1-api_auth_validate_phone_ip&t59.qf=1-api_captcha_force&t59.qf=1-api_oauth_direct_flood&t59.qf=1-login_auth_wrap&t59.qf=1-login_vk_com&t59.qf=1-login_vk_login&t59.qf=1-m_login_default&t59.qf=1-restore_forgot_non_ads_login_ua_member&t59.qf=3-first_show&t60.s=captcha_result&t60.cn=С_Результат%20ввода%20капчи&t60.qw=count&t60.g=300&t60.qb=3&t61.s=al_login_requests_times_pct&t61.cn=AA_Время%20работы%20методов%20на%20домене%20oauth.vk.com&t61.qw=p99&t61.g=300&t61.qf=2-act%2Btotal&t62.s=login_requests_times_pct&t62.cn=AA_Время%20работы%20ajax-методов%20используемых%20при%20регистрациях%20sts_times_pct%3A%20p99&t62.qw=p99&t62.g=300&t63.s=login_vk_requests_times_pct&t63.cn=AA_Время%20работы%20методов%20на%20домене%20login.vk.com&t63.qw=p99&t63.g=300&t64.s=api_oauth_requests_times_pct&t64.cn=AA_Время%20работы%20методов%20на%20домене%20oauth.vk.com&t64.qw=p99&t64.g=300&t65.s=api_auth_requests_times_pct&t65.cn=AA_Время%20работы%20методов%20секции%20api%20auth&t65.qw=p99&t65.g=300&t66.s=connect_vk_requests_times_pct&t66.cn=AA_Время%20работы%20методов%20на%20домене%20id.vk.com&t66.qw=p99&t66.g=300&t67.s=api_oauth_requests_times_pct_total&t67.cn=AA_WD_Ошибки%20методов%20на%20домене%20oauth.vk.com&t67.qw=count&t67.g=300&t67.qf=3-fail&t68.s=al_login_requests_times_pct_total&t68.cn=AA_WD_Ошибки%20ajax-методов%20используемых%20при%20регистрациях%20&t68.qw=count&t68.g=300&t68.qf=3-fail&t69.s=connect_vk_requests_times_pct_total&t69.cn=AA_Ошибки%20методов%20на%20домене%20id.vk.com&t69.qw=count&t69.g=300&t69.qf=3-fail&t70.s=api_auth_requests_times_pct_total&t70.cn=AA_Ошибки%20методов%20секции%20api%20auth&t70.qw=count&t70.g=300&t70.qf=3-fail&t71.s=check_user_action_usage&t71.cn=ACC_Использование%20механизма%20CUA&t71.qw=count&t71.g=300&t72.s=check_user_action_enter&t72.cn=ACC_События%20начала%20прохождения%20критического%20действия%20пользователя%20%28CUA%29&t72.qw=count&t72.g=300&t72.qb=1&t73.s=check_user_action_result&t73.cn=ACC_События%20прохождения%20критического%20действия%20пользователя%20%28CUA%29&t73.qw=count&t73.g=300&t73.qb=1&t74.s=deactivate_service&t74.cn=ACC_Отключение%20сервиса%20VK%20в%20ЛК%20%28вкладка%20“Сервисы%20и%20сайты”%29&t74.qw=count&t74.g=300&t75.s=deactivated_users&t75.cn=ACC_WD_Удаление%20аккаунта%20пользователя&t75.qw=count&t75.g=300&t75.qf=5-deactivated&t76.s=deactivated_users&t76.cn=ACC_WD_Восстановление%20аккаунта%20пользователя&t76.qw=count&t76.g=300&t76.qf=5-restored&t77.s=restore2_reset_session&t77.cn=ACC_WD_Сбросы%20одной%20сессии%20пользователя&t77.qw=count&t77.g=300&t77.qf=1-one&t78.s=restore2_reset_session&t78.cn=ACC_WD_Сбросы%20всех%20сессий%20пользователя&t78.qw=count&t78.g=300&t78.qf=1-all&t79.s=member_phone_set&t79.cn=ACC_Привязка%20номера%20пользователя&t79.qw=count&t79.g=300&t79.qf=1-new&t80.s=member_phone_set&t80.cn=ACC_Изменение%20номера%20пользователя&t80.qw=count&t80.g=300&t80.qf=1-change&t81.s=email_events&t81.cn=ACC_Операции%20с%20почтовыми%20адресами%20аккаунтов&t81.qw=count&t81.g=300&t81.qb=1&t81.qf=1-confirmation&t81.qf=1-unbinding&t82.s=auth_password_change&t82.cn=ACC_Смена%20пароля&t82.qw=count&t82.g=300&t83.s=otp_auth&t83.cn=ACC_Включение%202fa&t83.qw=count&t83.g=300&t83.qf=1-enable&t84.s=otp_auth&t84.cn=ACC_Отключение%202fa&t84.qw=count&t84.g=300&t84.qf=1-disable&t85.s=apps_api_errors&t85.cn=ACC_Ошибки%20API&t85.qw=count&t85.g=300&t85.qb=2&t85.qf=1-%207344294&t85.qf=2-account.checkPassword&t85.qf=2-account.deactivate&t85.qf=2-account.getCanChangePhone&t85.qf=2-account.getHelpHints&t85.qf=2-account.getProfileInfo&t85.qf=2-account.getProfileNavigationInfo&t85.qf=2-account.getSecurityAlerts&t85.qf=2-account.getSettings&t85.qf=2-account.getSilentModeStatus&t85.qf=2-account.reactivate&t85.qf=2-account.saveProfileInfo&t85.qf=2-account.touchQuestionnaire&t85.qf=2-account.validateProfileInfo&t86.s=account_verification&t86.cn=ACC_Верификация%20аккаунта&t86.g=900&t87.s=auth_password_change&t87.cn=ACC_Изменения%20пароля%20в%20ЛК&t87.qw=count&t87.g=300&t87.qf=1-api.account&t87.qf=1-api.settings&t88.s=auth_password_change&t88.cn=ACC_Изменения%20пароля%20в%20CUA%202.0%20%28ЛК%29&t88.qw=count&t88.g=900&t88.qf=1-api.cua&t89.s=vkpay_api_callback&t89.qw=count&t89.g=300&t89.qb=1&t90.s=connect_watcher_user_auth_events&t90.cn=AT_Auth%20-%20успешные%20верификации%20фактора%20владения%20-%20android&t90.qw=count&t90.g=900&t90.qf=2-auth_phone_confirmed&t90.qf=3-admin_android&t90.qf=3-android&t90.qf=3-android_external&t90.qf=3-android_messenger&t90.qf=3-android_webview_external&t91.s=connect_watcher_user_auth_events&t91.cn=AT_Auth%20-%20успешные%20верификации%20фактора%20владения%20-%20ios&t91.qw=count&t91.g=900&t91.qf=2-auth_phone_confirmed&t91.qf=3-ios_external&t91.qf=3-ipad_v2&t91.qf=3-iphone&t91.qf=3-iphone_messenger&t92.s=connect_watcher_user_auth_events&t92.cn=AT_Auth%20-%20успешные%20верификации%20фактора%20владения%20-%20web&t92.qw=count&t92.g=900&t92.qf=2-auth_phone_confirmed&t92.qf=3-android_webview_external&t92.qf=3-api&t92.qf=3-iphone_webview_external&t92.qf=3-mvk&t92.qf=3-mvk_external&t92.qf=3-web2&t92.qf=3-web_external&t93.s=connect_watcher_user_auth_events&t93.cn=AT_Auth%20-%20отправки%20экосистемных%20otp-пушей&t93.qw=count&t93.g=900&t93.qf=2-auth_phone_sent_ecosystem_push&t94.s=connect_watcher_user_auth_events&t94.cn=AT_Auth%20-%20отправки%20экосистемных%20otp-писем&t94.qw=count&t94.g=300&t94.qf=2-auth_phone_sent_email&t95.s=api_errors&t95.cn=AT_Auth%20-%20API%20-%20ошибки%20на%20auth.validatePhone&t95.qw=count&t95.g=300&t95.qf=1-auth&t95.qf=2-auth.validatePhone&t96.s=connect_watcher_user_auth_events&t96.cn=AT_Auth%20-%20отправки%20звоноков-сбросов%20с%20нашего%20бэка&t96.qw=count&t96.g=3600&t96.qf=2-auth_phone_sent_call&t97.s=api_methods&t97.cn=AT_Auth%20-%20API%20-%20запросы%20к%20auth.validatePhone&t97.qw=count&t97.g=900&t97.qf=1-auth&t97.qf=2-validatePhone&t98.s=authcheck_pass&t98.cn=AT_Auth%20-%20успешные%20вводы%20OTP-кодов%20-%20sms&t98.qw=count&t98.g=3600&t98.qf=3-sms&t99.s=authcheck_pass&t99.cn=AT_Auth%20-%20успешные%20вводы%20OTP-кодов%20-%20ecosystem%20push&t99.qw=count&t99.g=900&t99.qf=3-ecosystem_push&t100.s=authcheck_pass&t100.cn=AT_Auth%20-%20успешные%20вводы%20OTP-кодов%20-%20email&t100.qw=count&t100.g=900&t100.qf=3-email&t101.s=authcheck_pass&t101.cn=AT_Auth%20-%20успешные%20вводы%20OTP-кодов%20-%20libverify&t101.qw=count&t101.g=900&t101.qf=3-libverify&t102.s=authcheck_pass&t102.cn=AT_Auth%20-%20успешные%20вводы%20OTP-кодов%20-%20звонок-сброс&t102.qw=count&t102.g=3600&t102.qf=3-call_reset&t103.s=authcheck_pass&t103.cn=AT_Auth%20-%20успешные%20вводы%20OTP-кодов%20-%20резервные%20коды&t103.qw=count&t103.g=3600&t103.qf=3-reserve_code&t104.s=code_authorization_events&t104.cn=AT_AUTH%20-%20Верификации%20через%20QR-код&t104.qw=count&t104.g=300&t104.qf=1-processAuthCode&t104.qf=2-success&t105.s=mrg_smsapi_calls&t105.cn=AT_AUTH%20-%20Верификации%20через%20SMSAPI&t105.qw=count&t105.g=900&t105.qb=3&t105.qf=1-verify&t105.qf=2-success&t105.qf=3-vk_otp_auth&t105.qf=3-vk_passwordless_auth&t105.qf=3-vk_registration&t106.s=apps_api&t106.cn=AT_AUTH%20-%20Вызовы%20ecosystem.*&t106.qw=count&t106.g=300&t106.qb=3&t106.qf=2-ecosystem&t107.s=connect_oauth_service_errors&t107.cn=C_OAuth%20service%20errors&t107.qw=count&t107.g=900&t107.qb=1&t107.qb=2&t108.s=connect_oauth_specific_errors&t108.cn=C_OAuth%20specific%20errors&t108.qw=count&t108.g=3600&t109.s=connect_oauth_external_login&t109.cn=С_OAuth%20VKID&t109.qw=count&t109.g=900&t110.s=connect_oauth_external_login&t110.cn=С_OAuth%20mobile%20SDK&t110.qw=count&t110.g=900&t110.qf=1-sdk&t111.s=connect_oauth_external_login&t111.cn=С_OAuth%20ESIA&t111.qw=count&t111.g=900&t111.qb=2&t111.qf=2~apple_id&t111.qf=2~google_id&t111.qf=2~my_com&t111.qf=2~ok_ru&t111.qf=2~sber_id&t111.qf=2~tinkoff_id&t111.qf=2~vk_id&t111.qf=2~yandex_id&t112.s=connect_oauth_external_login&t112.cn=С_OAuth%20Sber%20ID&t112.qw=count&t112.g=900&t112.qb=2&t112.qf=2~apple_id&t112.qf=2~esia&t112.qf=2~google_id&t112.qf=2~my_com&t112.qf=2~ok_ru&t112.qf=2~tinkoff_id&t112.qf=2~vk_id&t112.qf=2~yandex_id&t113.s=connect_oauth_external_login&t113.cn=С_OAuth%20Tinkoff%20ID&t113.qw=count&t113.g=3600&t113.qb=2&t113.qf=2~apple_id&t113.qf=2~esia&t113.qf=2~google_id&t113.qf=2~my_com&t113.qf=2~ok_ru&t113.qf=2~sber_id&t113.qf=2~vk_id&t113.qf=2~yandex_id&t114.s=connect_oauth_external_login&t114.cn=С_OAuth%20Yandex%20ID&t114.qw=count&t114.g=3600&t114.qb=2&t114.qf=2~apple_id&t114.qf=2~esia&t114.qf=2~google_id&t114.qf=2~my_com&t114.qf=2~ok_ru&t114.qf=2~sber_id&t114.qf=2~tinkoff_id&t114.qf=2~vk_id&t115.s=connect_oauth_external_login&t115.cn=С_OAuth%20ok.ru&t115.qw=count&t115.g=3600&t115.qb=2&t115.qf=2~apple_id&t115.qf=2~esia&t115.qf=2~google_id&t115.qf=2~my_com&t115.qf=2~sber_id&t115.qf=2~tinkoff_id&t115.qf=2~vk_id&t115.qf=2~yandex_id&t116.s=connect_oauth_external_login&t116.cn=С_OAuth%20my.com&t116.qw=count&t116.g=3600&t116.qb=2&t116.qf=2~apple_id&t116.qf=2~esia&t116.qf=2~google_id&t116.qf=2~ok_ru&t116.qf=2~sber_id&t116.qf=2~tinkoff_id&t116.qf=2~vk_id&t116.qf=2~yandex_id&t117.s=connect_oauth_external_login&t117.cn=С_OAuth%20Google%20ID&t117.qw=count&t117.g=3600&t117.qb=2&t117.qf=2~apple_id&t117.qf=2~esia&t117.qf=2~my_com&t117.qf=2~ok_ru&t117.qf=2~sber_id&t117.qf=2~tinkoff_id&t117.qf=2~vk_id&t117.qf=2~yandex_id&t118.s=connect_oauth_external_login&t118.cn=С_OAuth%20Apple%20ID&t118.qw=count&t118.g=3600&t118.qb=2&t118.qf=2~esia&t118.qf=2~google_id&t118.qf=2~my_com&t118.qf=2~ok_ru&t118.qf=2~sber_id&t118.qf=2~tinkoff_id&t118.qf=2~vk_id&t118.qf=2~yandex_id&v0.n=environment&v0.l=1.0-3.0-4.0-5.0-6.0-7.0-8.0-9.0-10.0-11.0-12.0-13.0-14.0-15.0-16.0-17.0-18.0-19.0-20.0-21.0-22.0-23.0-24.0-25.0-26.0-27.0-28.0-29.0-30.0-31.0-32.0-33.0-34.0-35.0-36.0-37.0-43.0-44.0-45.0-46.0-47.0-48.0-49.0-50.0-51.0-52.0-53.0-54.0-55.0-56.0-58.0-59.0-60.0-61.0-62.0-63.0-64.0-65.0-66.0-67.0-68.0-69.0-70.0-71.0-72.0-73.0-74.0-75.0-76.0-77.0-78.0-79.0-80.0-81.0-82.0-83.0-84.0-85.0-89.0-90.0-91.0-92.0-93.0-94.0-95.0-96.0-97.0-98.0-99.0-100.0-101.0-102.0-103.0-104.0-105.0&v0.s0.s=api_methods&v0.s0.t=1&v0.s0.qf=1-adsint&v0.s0.qf=2-getPrivacySettings&v0.s0.qf=2-registerAdEvents&v0.s0.qf=5~kate_mobile&v0.s0.qf=5~vk_video_ios&v0.s1.s=api_methods&v0.s1.t=4&v0.s1.qf=1-groups&v0.s1.qf=1-photos&v0.s1.qf=5-iphone_clips&v0.s1.qf=2~addRecent&v0.s1.qf=2~getBanner&v0.s1.qf=2~getOnboardingState&v0.s1.qf=4~190&v0.s1.qf=4~192&v0.s1.qf=4~195&v0.s1.qf=4~205&v0.s1.qf=4~211&v0.s1.qf=4~214&v0.s1.qf=4~216&v0.s1.qf=4~217&v0.s1.qf=4~219&v0.s1.qf=4~220&v0.s1.qf=4~221&v0.s1.qf=4~222&v0.s1.qf=4~223&v0.s1.qf=4~224&v0.s1.qf=4~225&v0.s1.qf=4~226&v0.s1.qf=4~227&v0.s1.qf=4~228&v0.s1.qf=4~229&v0.s1.qf=4~230&v0.s1.qf=4~231&v0.s1.qf=4~232&v1.n=var1&v.environment=production&v.environment=staging';
const u = [...new URLSearchParams(s).entries()];

const p = urlDecode(toTreeObj(arrToObj(u)));

const p2 = produce(p, (p) => {
  delete p.plots['0'];
  delete p.plots['10'];
  p.orderPlot.splice(p.orderPlot.indexOf('0'), 1);
  p.orderPlot.splice(p.orderPlot.indexOf('10'), 1);
  const v = p.variables['0'];
  if (v) {
    delete v.source[0];
  }
});

const u2 = urlEncode(p2, p);

console.log(u2);

const p3 = urlDecode(toTreeObj(arrToObj(u2)), p);

console.log(p3, p);
*/
