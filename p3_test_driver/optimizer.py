# Written by Claudio Fahey (claudio.fahey@emc.com)

from __future__ import division

import logging
import uuid
import os
import itertools
import math
import pandas as pd
import numpy as np
import six
import datetime
import traceback
import re

from json_util import save_json_to_file, load_json_from_file
from system_command import time_duration_to_seconds

import simplejson as json

from moe.easy_interface.experiment import Experiment
from moe.easy_interface.simple_endpoint import gp_next_points, gp_hyper_opt, call_endpoint_with_payload
from moe.optimal_learning.python.data_containers import SamplePoint, HistoricalData
from moe.views.constant import ALL_REST_ROUTES_ROUTE_NAME_TO_ENDPOINT, GP_NEXT_POINTS_EPI_ROUTE_NAME, GP_MEAN_VAR_ROUTE_NAME, GP_HYPER_OPT_ROUTE_NAME, GP_EI_ROUTE_NAME
from moe.views.schemas.gp_next_points_pretty_view import GpNextPointsResponse
from moe.views.schemas.rest.gp_hyper_opt import GpHyperOptResponse
from moe.views.schemas.rest.gp_mean_var import GpMeanVarResponse
from moe.views.schemas.rest.gp_ei import GpEiResponse
from moe.optimal_learning.python.python_version.covariance import SquareExponential

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 6543

def moe_gp_next_points(
        moe_experiment,
        method_route_name=GP_NEXT_POINTS_EPI_ROUTE_NAME,
        rest_host=DEFAULT_HOST,
        rest_port=DEFAULT_PORT,
        testapp=None,
        **kwargs):
    """Hit the rest endpoint for finding next point of highest EI at rest_host:rest_port corresponding to the method with the given experiment.
    Identical to gp_next_points except that it returns all fields."""
    endpoint = ALL_REST_ROUTES_ROUTE_NAME_TO_ENDPOINT[method_route_name]
    raw_payload = kwargs.copy()  # Any options can be set via the kwargs ('covariance_info' etc.)
    experiment_payload = moe_experiment.build_json_payload()
    if 'gp_historical_info' not in raw_payload:
        raw_payload['gp_historical_info'] = experiment_payload.get('gp_historical_info')
    if 'domain_info' not in raw_payload:
        raw_payload['domain_info'] = experiment_payload.get('domain_info')
    json_payload = json.dumps(raw_payload)
    json_response = call_endpoint_with_payload(rest_host, rest_port, endpoint, json_payload, testapp)
    output = GpNextPointsResponse().deserialize(json_response)
    return output

def moe_gp_mean_var(
        points_sampled,
        points_to_evaluate,
        rest_host=DEFAULT_HOST,
        rest_port=DEFAULT_PORT,
        testapp=None,
        **kwargs
    ):
    """Hit the rest endpoint for calculating the posterior mean and variance of a gaussian process, given points already sampled.
    Identical to gp_mean_var except that it returns all fields."""
    endpoint = ALL_REST_ROUTES_ROUTE_NAME_TO_ENDPOINT[GP_MEAN_VAR_ROUTE_NAME]
    raw_payload = kwargs.copy()  # Any options can be set via the kwargs ('covariance_info' etc.)
    raw_payload['points_to_evaluate'] = points_to_evaluate
    # Sanitize input points
    points_sampled_clean = [SamplePoint._make(point) for point in points_sampled]
    historical_data = HistoricalData(
            len(points_to_evaluate[0]),  # The dim of the space
            sample_points=points_sampled_clean,
            )
    if 'gp_historical_info' not in raw_payload:
        raw_payload['gp_historical_info'] = historical_data.json_payload()
    if 'domain_info' not in raw_payload:
        raw_payload['domain_info'] = {'dim': len(points_to_evaluate[0])}
    json_payload = json.dumps(raw_payload)
    json_response = call_endpoint_with_payload(rest_host, rest_port, endpoint, json_payload, testapp)
    output = GpMeanVarResponse().deserialize(json_response)
    return output

def moe_gp_ei(
        points_sampled,
        points_to_evaluate,
        rest_host=DEFAULT_HOST,
        rest_port=DEFAULT_PORT,
        testapp=None,
        **kwargs
    ):
    """Hit the rest endpoint for calculating the expected improvement of a gaussian process, given points already sampled."""
    endpoint = ALL_REST_ROUTES_ROUTE_NAME_TO_ENDPOINT[GP_EI_ROUTE_NAME]
    raw_payload = kwargs.copy()  # Any options can be set via the kwargs ('covariance_info' etc.)
    raw_payload['points_to_evaluate'] = points_to_evaluate
    # Sanitize input points
    points_sampled_clean = [SamplePoint._make(point) for point in points_sampled]
    historical_data = HistoricalData(
            len(points_to_evaluate[0]),  # The dim of the space
            sample_points=points_sampled_clean,
            )
    if 'gp_historical_info' not in raw_payload:
        raw_payload['gp_historical_info'] = historical_data.json_payload()
    if 'domain_info' not in raw_payload:
        raw_payload['domain_info'] = {'dim': len(points_to_evaluate[0])}
    json_payload = json.dumps(raw_payload)
    json_response = call_endpoint_with_payload(rest_host, rest_port, endpoint, json_payload, testapp)
    output = GpEiResponse().deserialize(json_response)
    return output

def moe_gp_hyper_opt(
        points_sampled,
        rest_host=DEFAULT_HOST,
        rest_port=DEFAULT_PORT,
        testapp=None,
        **kwargs
        ):
    """Hit the rest endpoint for optimizing the hyperparameters of a gaussian process, given points already sampled.
    Identical to gp_hyper_opt except that it returns all fields."""
    endpoint = ALL_REST_ROUTES_ROUTE_NAME_TO_ENDPOINT[GP_HYPER_OPT_ROUTE_NAME]
    # This will fail if len(points_sampled) == 0; but then again this endpoint doesn't make sense with 0 historical data
    gp_dim = len(points_sampled[0][0])
    raw_payload = kwargs.copy()
    # Sanitize input points
    points_sampled_clean = [SamplePoint._make(point) for point in points_sampled]
    historical_data = HistoricalData(
            gp_dim,
            sample_points=points_sampled_clean,
            )
    if 'domain_info' not in raw_payload:
        raw_payload['domain_info'] = {'dim': gp_dim}
    if 'gp_historical_info' not in raw_payload:
        raw_payload['gp_historical_info'] = historical_data.json_payload()
    if 'hyperparameter_domain_info' not in raw_payload:
        hyper_dim = gp_dim + 1  # default covariance has this many parameters
        raw_payload['hyperparameter_domain_info'] = {
            'dim': hyper_dim,
            'domain_bounds': [{'min': 0.1, 'max': 2.0}] * hyper_dim,
        }
    json_payload = json.dumps(raw_payload)
    json_response = call_endpoint_with_payload(rest_host, rest_port, endpoint, json_payload, testapp)
    output = GpHyperOptResponse().deserialize(json_response)
    return output

class Optimizer(object):
    def __init__(self, transform_col_rules=None, to_minimize=None, observations_file=None, extra_points_file=None,
            hyperparameter_alpha_domain=None, minimize_col_stddev_domain=(None,None),
            grid_search=True, grid_search_max_size=None, max_get_point_attempts=10, min_sample_size=3, max_num_points_since_hyper_opt=5,
            max_moe_iterations=1000, moe_rest_host='127.0.0.1', moe_rest_port=DEFAULT_PORT,
            read_only=False):
        """Note that defaults for P3 are in p3_test_optimizer.py."""

        self.transform_col_rules = transform_col_rules
        self.minimize_col = to_minimize
        self.observations_file = observations_file
        self.extra_points_file = extra_points_file
        self.hyperparameter_alpha_domain = hyperparameter_alpha_domain
        self.grid_search = grid_search
        self.grid_search_max_size = grid_search_max_size
        self.max_get_point_attempts = max_get_point_attempts
        self.min_sample_size = min_sample_size
        self.max_num_points_since_hyper_opt = max_num_points_since_hyper_opt
        self.max_moe_iterations = max_moe_iterations
        self.moe_rest_host = moe_rest_host
        self.moe_rest_port = moe_rest_port
        self.read_only = read_only
        self.minimize_col_stddev_domain = minimize_col_stddev_domain

        if self.hyperparameter_alpha_domain is None:
            self.hyperparameter_alpha_domain = [0.02, 4.0]

        self.minimize_col_transformer = MinimizeColTransformer()
        self.avoid_value = np.nan
        self.avoid_var = np.nan
        self.grid_search_iteration = 0
        self.moe_iteration = 0
        self.observations_df = pd.DataFrame()
        self.samples_df = pd.DataFrame()
        self.valid_samples_df = pd.DataFrame()
        self.extra_points_df = pd.DataFrame()
        self.status_str = ''
        self.best_sample = None
        self.num_points_since_hyper_opt = np.inf
        self.covariance_info = {}
        self.gp_hyper_opt_response = None
        self.gp_hyper_opt_sec = np.nan

        self.load_config_from_header()

        self.all_transformers = [get_rule_transformer(r) for r in self.transform_col_rules]
        self.transformers = [t for t in self.all_transformers if t.fixed_value is None]
        self.fixed_transformers = [t for t in self.all_transformers if not t.fixed_value is None]
        self.transform_col_names = [t.name for t in self.transformers]
        self.transformers_dict = {t.name: t for t in self.all_transformers}

        if self.grid_search_max_size is None:
            # Recommended number of points to bootstrap MOE is 2*D.
            # http://yelp.github.io/MOE/faq.html#how-do-i-bootstrap-moe-what-initial-data-does-it-need
            self.grid_search_max_size = 2 * len(self.transformers)

        self.build_grid_search_points()

    def load_config_from_header(self):
        if self.observations_file and os.path.isfile(self.observations_file):
            df = pd.DataFrame(load_json_from_file(self.observations_file))
            if '_header' in df:
                header = df.loc[~df['_header'].isnull(),'_header'].ix[0]
                logging.debug('Optimizer.load_config_from_header: header=%s' % header)
                if not self.transform_col_rules and 'transform_col_rules' in header:
                    self.transform_col_rules = header['transform_col_rules']
                if not self.minimize_col and 'minimize_col' in header:
                    self.minimize_col = header['minimize_col']
                if not self.covariance_info and 'covariance_info' in header:
                    self.covariance_info = header['covariance_info']
                    logging.info('Optimizer.load_config_from_header: Loaded covariance info: %s' % self.covariance_info)
                self.num_points_since_hyper_opt = header.get('num_points_since_hyper_opt', np.inf)
                return True
        return False

    def load(self):
        """Load historical observations from disk."""
        if self.observations_file and os.path.isfile(self.observations_file):
            df = pd.DataFrame(load_json_from_file(self.observations_file))
            if '_header' in df:
                df = df[df['_header'].isnull()]            
                del df['_header']
            logging.info('Optimizer.load: Read %d observations from %s' % (len(df), self.observations_file))
            self.raw_observations_df = df.copy()
            for t in self.all_transformers:
                if not t.value_if_missing is None:
                    if t.name is df:
                        df[t.name] = df[t.name].fillna(t.value_if_missing)
                    else:
                        df[t.name] = t.value_if_missing
            self.observations_df = df
            # self.debug_print()

        if self.extra_points_file and os.path.isfile(self.extra_points_file):
            try:
                df = pd.DataFrame(load_json_from_file(self.extra_points_file))
                if '_header' in df:
                    df = df[df['_header'].isnull()]            
                    del df['_header']                
                df = df[self.transform_col_names]
                logging.info('Optimizer.load: Read %d extra points from %s' % (len(df), self.extra_points_file))
                self.extra_points_df = df
            except:
                logging.warn('Disregarding extra points file due to exception: %s' % traceback.format_exc())
            logging.debug('Optimizer.load: extra_points_df:\n%s' % self.extra_points_df)

    def save(self):
        if not self.read_only:
            recs = [{'_header': {
                'transform_col_rules': self.transform_col_rules,
                'minimize_col': self.minimize_col,
                'covariance_info': self.covariance_info,
                'num_points_since_hyper_opt': self.num_points_since_hyper_opt,
                }}]
            recs.extend(self.observations_df.to_dict(orient='records'))
            save_json_to_file(recs, self.observations_file, sort_keys=True, indent=True)

    def next_point_to_sample(self):
        """This is the primary method that users of this class should use. 
        It will return a dict of parameters (points) to sample along with the number of new observations needed (sample size).
        It will first iterate using a grid search, then use MOE to select subsequent points."""

        # Build samples from observations
        self.build_samples()

        p = None
        optimization_info = {}

        # Grid Search
        while self.grid_search_iteration < self.grid_search_size:
            p = tuple(self.grid_search_points[self.grid_search_iteration])
            self.grid_search_iteration += 1   # First value will be 1.

            # See if we already sampled this point.
            try:
                historical_sample = self.samples_df.loc[p,:]
            except KeyError:
                # We have no observations at this point.
                logging.debug('Optimizer.next_point_to_sample: We have no observations at point %s.' % (p,))
                needed_new_observations = self.min_sample_size
                break
            # We have at least one observation but determine if it is enough.
            # If we are avoiding this point, no more observations are needed.
            if isinstance(historical_sample, pd.DataFrame):
                historical_sample = historical_sample.iloc[0]                
            if not historical_sample['optimizer_avoid_point']:
                needed_new_observations = self.min_sample_size - historical_sample['count']
                if needed_new_observations > 0:
                    # We need at least one more observation.
                    logging.debug('Optimizer.next_point_to_sample: We need at least one more observation at point %s.' % (p,))
                    break
            # We don't need any more observations at this point. Repeat loop to try the next point in the grid search.
            p = None

        # MOE
        if p is None:
            self.moe_iteration += 1    # First value will be 1.
            if self.moe_iteration > self.max_moe_iterations:
                logging.info('Optimizer.next_point_to_sample: Exceeded max_moe_iterations')
                return None, 0, optimization_info
            get_point_attempt = 0
            while True:
                # Get real point from MOE
                pr, optimization_info = self.next_point_to_sample_real()
                # Convert from real
                p = self.point_from_real(pr)
                # See if we already sampled this point.
                try:
                    historical_sample = self.samples_df.loc[p,:]
                except KeyError:
                    # We have no observations at this point.
                    needed_new_observations = self.min_sample_size
                    break
                if isinstance(historical_sample, pd.DataFrame):
                    historical_sample = historical_sample.iloc[0]
                # We have at least one observation but determine if it is enough.
                # If we are avoiding this point, no more observations are needed.
                if not historical_sample['optimizer_avoid_point']:
                    needed_new_observations = self.min_sample_size - historical_sample['count']
                    if needed_new_observations > 0:
                        # We need at least one more observation.
                        break
                # Got a duplicate point. Add exact real point to extra_points and repeat loop to call MOE again.
                logging.debug('Optimizer.next_point_to_sample: Duplicate point p=%s, get_point_attempt=%d' % 
                    (p, get_point_attempt))
                self.add_extra_points([pr])
                # If we have looped too many times, return None.
                get_point_attempt += 1
                if get_point_attempt >= self.max_get_point_attempts:
                    logging.info('Optimizer.next_point_to_sample: Exceeded max_get_point_attempts')
                    return None, 0, optimization_info
            self.num_points_since_hyper_opt += 1.0
        
        assert not p is None

        # Convert point to dict
        pdict = {t.name: x for t,x in zip(self.transformers, p)}

        # Combine with fixed transformers
        pdict_fixed = {t.name: t.from_real(t.to_real(t.fixed_value)) for t in self.fixed_transformers}
        pdict.update(pdict_fixed)

        # Don't want to use numpy.bool_ because it gives errors with JSON serialization.
        pdict = {k: (bool(v) if isinstance(v,np.bool_) else v) for k,v in pdict.iteritems()}

        self.build_status(pdict, optimization_info)

        return pdict, needed_new_observations, optimization_info

    def build_status(self, pdict, optimization_info):
        """Reflects the status based on the point that is being returned by next_point_to_sample()."""
        self.build_status_str()

        next_point = pd.Series(name='next_point')
        next_point_real = pd.Series(name='next_point_real')
        hp = pd.Series(name='hyperparameters')
        hp_real = pd.Series(name='hyperparameters_real')
        gll = pd.Series(name='grad_log_likelihood')

        for col in self.transform_col_names:
            next_point[col] = pdict[col]

        try:
            for i, col in enumerate(self.transform_col_names):
                next_point_real[col] = optimization_info['optimizer_point_to_sample_real'][i]
        except:
            pass

        mean = np.nan
        var = np.nan
        try:
            mean = optimization_info['optimizer_gp_mean_var_response']['mean'][0]
            var = optimization_info['optimizer_gp_mean_var_response']['var'][0][0]
        except:
            pass
        next_point_real['signal_mean'], next_point_real['signal_var'] = mean, var
        next_point_real['signal_std'] = np.sqrt(next_point_real['signal_var'])
        next_point['signal_mean'], next_point['signal_var'] = self.minimize_col_transformer.denormalize_mean_var(mean, var)        
        next_point['signal_std'] = np.sqrt(next_point['signal_var'])

        ei = np.nan
        try:
            ei = optimization_info['optimizer_gp_next_points_response']['status']['expected_improvement']
        except:
            pass
        next_point_real['expected_improvement'] = ei
        next_point['expected_improvement'] = self.minimize_col_transformer.denormalize_delta(ei)

        success = False
        try:
            success = optimization_info['optimizer_gp_next_points_response']['status']['optimizer_success']['gradient_descent_tensor_product_domain_found_update']
        except:
            pass            
        next_point['optimization_success'] = success
        next_point_real['optimization_success'] = success

        optimization_sec = np.nan
        try:
            optimization_sec = optimization_info['optimizer_gp_next_points_sec']
        except:
            pass
        next_point['optimization_sec'] = optimization_sec
        next_point_real['optimization_sec'] = optimization_sec
        
        try:
            hyperparameters = optimization_info['optimizer_gp_hyper_opt_response']['covariance_info']['hyperparameters']
            for i, t in enumerate(self.transformers):
                col = t.name
                hp_real[t.name] = hyperparameters[i+1]
                hp[t.name] = t.denormalize_delta(hyperparameters[i+1])
            hp_real['signal_mean'], hp_real['signal_var'] = 0.0, hyperparameters[0]
            hp_real['signal_std'] = np.sqrt(hp_real['signal_var'])
            hp['signal_mean'], hp['signal_var'] = self.minimize_col_transformer.denormalize_mean_var(hp_real['signal_mean'], hp_real['signal_var'])
            hp['signal_std'] = np.sqrt(hp['signal_var'])
        except:
            pass

        success = False
        try:
            success = optimization_info['optimizer_gp_hyper_opt_response']['status']['optimizer_success']['log_marginal_likelihood_newton_found_update']
        except:
            pass
        hp['optimization_success'] = success
        hp_real['optimization_success'] = success

        optimization_sec = np.nan
        try:
            optimization_sec = optimization_info['optimizer_gp_hyper_opt_sec']
        except:
            pass
        hp['optimization_sec'] = optimization_sec
        hp_real['optimization_sec'] = optimization_sec

        log_likelihood = np.nan
        try:
            grad_log_likelihood = optimization_info['optimizer_gp_hyper_opt_response']['status']['grad_log_likelihood']
            for i, col in enumerate(self.transform_col_names):
                gll[col] = grad_log_likelihood[i+1]
            gll['signal_std'] = gll['signal_var'] = grad_log_likelihood[0]
            log_likelihood = optimization_info['optimizer_gp_hyper_opt_response']['status']['log_likelihood']
        except:
            pass
        hp['log_likelihood'] = log_likelihood
        hp_real['log_likelihood'] = log_likelihood

        # Combine into a single data frame
        df = pd.DataFrame([next_point, next_point_real, hp, hp_real, gll])
        df = df[self.transform_col_names + [
            'signal_std','signal_var','signal_mean','expected_improvement','log_likelihood', 'optimization_success','optimization_sec']]
        df = df.T
        logging.info('Optimizer.build_status: status_df:\n%s' % df)
        self.status_df = df

    def build_status_str(self):
        """Reflects the status based on the point that is being returned by next_point_to_sample()."""
        # Note that first iterations have value of 1.
        if self.moe_iteration <= 0:
            self.status_str = 'grid search iteration %d of %d (%.0f %%)' % (
                self.grid_search_iteration, self.grid_search_size,
                100.0 * (self.grid_search_iteration - 1) / self.grid_search_size)
        else:
            self.status_str = 'optimization iteration %d of %d (%.0f %%)' % (
                self.moe_iteration, self.max_moe_iterations,
                100.0 * (self.moe_iteration - 1) / self.max_moe_iterations)

    def get_status_str(self):
        return self.status_str

    def get_status_df(self):
        return self.status_df

    def add_extra_points(self, pr_list):
        df = pd.DataFrame(pr_list, columns=self.transform_col_names)
        self.extra_points_df = pd.concat([self.extra_points_df, df], ignore_index=True)
        if self.extra_points_file and not self.read_only:
            recs = [{'_header': {'transform_col_rules': self.transform_col_rules}}]
            recs.extend(self.extra_points_df.to_dict(orient='records'))
            save_json_to_file(recs, self.extra_points_file, sort_keys=True, indent=True)

    def add_observations(self, observations):
        """Called by users of this class to record one or more test results."""
        df = pd.DataFrame(observations)
        if not self.minimize_col in df:
            df[self.minimize_col] = np.nan
        if not 'optimizer_avoid_point' in df:
            df['optimizer_avoid_point'] = False
        df.loc[df.optimizer_avoid_point.isnull(), 'optimizer_avoid_point'] = False
        self.observations_df = pd.concat([self.observations_df, df], ignore_index=True)
        if self.observations_file:
            self.save()
        cols = [t.name for t in self.transformers] + [self.minimize_col, 'optimizer_avoid_point']
        logging.info('Optimizer: Added observation(s):\n%s' % self.observations_df.tail(len(df))[cols])

    def build_samples(self):
        """Build unscaled/untransformed samples from observations."""
        if self.observations_df.empty:
            self.samples_df = pd.DataFrame()
        else:
            # Round trip observation points through transformers to discretize points.
            round_trip_df = self.observations_df.copy()
            for t in self.transformers:
                round_trip_df[t.name] = t.from_real_array(t.to_real_array(round_trip_df[t.name]))
            cols = [t.name for t in self.transformers] + [self.minimize_col, 'optimizer_avoid_point']
            logging.info('Optimizer.build_samples: round_trip_df (last 5): \n%s' % round_trip_df.tail(5)[cols])

            # Aggregate observations into samples.
            groupby_cols = [t.name for t in self.transformers]
            grp = round_trip_df.groupby(groupby_cols)
            agg_dict = {self.minimize_col: ['mean','var','std'], 'optimizer_avoid_point': ['max','count']}
            df = grp.agg(agg_dict)
            column_rename_dict = {
                (self.minimize_col,'mean'): 'mean',
                (self.minimize_col,'var'): 'var',
                (self.minimize_col,'std'): 'std',
                ('optimizer_avoid_point','max'): 'optimizer_avoid_point',
                ('optimizer_avoid_point','count'): 'count',
                }
            df.columns = [column_rename_dict.get(c,c) for c in df.columns.values]

            df.loc[df.optimizer_avoid_point, ['mean','var','std']] = [np.inf, 0.0, 0.0]

            df['std'] = df['std'].clip(*self.minimize_col_stddev_domain)
            df['var'] = df['std']**2

            # Somehow, we are getting negative variances!
            df.loc[df['var'] < 0,'var'] = 0.0

            self.samples_df = df

        if not self.samples_df.empty:
            # Valid samples are those with a non-null variance (sample size >= 2).
            # These are sent to MOE as historical samples.
            # TODO: should we consider samples with sample size smaller than min_sample_size?
            self.valid_samples_df = self.samples_df[~self.samples_df['var'].isnull()]
            if not self.valid_samples_df.empty:
                self.best_sample_df = self.valid_samples_df.sort(['mean']).head(1)
                self.best_sample = self.best_sample_df.iloc[0]
            logging.info('Optimizer.build_samples: samples_df (best 10):\n%s' % self.samples_df.sort(['mean']).head(10))

    def domain_bounds(self):
        return [t.bounds_real() for t in self.transformers]

    def hyperparameter_domain_bounds(self):
        alpha = [self.hyperparameter_alpha_domain]
        length_scales = [t.hyperparameter_bounds_real() for t in self.transformers]
        return alpha + length_scales
 
    def point_from_real(self, pr):
        p = tuple(map(lambda (t,r): t.from_real(r), zip(self.transformers, pr)))
        return p

    def point_to_real(self, p):
        pr = tuple(map(lambda (t,x): t.to_real(x), zip(self.transformers, p)))
        return pr

    def adjust_minimize_col_scale(self):
        """Adjust scaling of minimize column based on samples.
        Note that large changes will require the variance hyperparameter to be re-optimized."""
        self.minimize_col_transformer = MinimizeColTransformer(self.valid_samples_df['mean'].values)

    def build_points_sampled(self):
        """Build samples of real Assumes values that build_samples() has already been called."""
        if self.valid_samples_df.empty:
            self.points_sampled = None
            self.points_sampled_df = pd.DataFrame()
        else:
            samp_df = self.valid_samples_df.reset_index()
            points_df = pd.DataFrame([pd.Series(t.to_real_array(samp_df[t.name].values), name=t.name) for t in self.transformers]).T
            points_df['mean'], points_df['var'] = self.minimize_col_transformer.normalize_mean_var(
                samp_df['mean'].values, samp_df['var'].values)
            # Add extra points
            for index, row in self.extra_points_df.iterrows():
                extra_point_series = row
                #logging.debug('points_sampled: extra_point=%s' % str(extra_point))
                p = self.point_from_real(extra_point_series.values)
                try:
                    # Find historical sample that matches extra point
                    historical_sample = self.valid_samples_df.loc[p,:]
                    mean, var = self.minimize_col_transformer.normalize_mean_var(
                        np.array([historical_sample['mean']]), np.array([historical_sample['var']]))
                    extra_point_series['mean'], extra_point_series['var'] = mean[0], var[0]
                    # logging.debug('Optimizer.points_sampled: extra_point_series=%s' % str(extra_point_series))
                    points_df = points_df.append(extra_point_series)
                except KeyError:
                    pass
            logging.debug('Optimizer.build_points_sampled: points_sampled_df=\n%s' % points_df.sort(['mean']))
            samples = zip(points_df[[t.name for t in self.transformers]].values, points_df['mean'].values, points_df['var'].values)
            self.points_sampled = samples
            self.points_sampled_df = points_df

    def build_grid_search_points(self):
        if self.grid_search and self.grid_search_max_size > 0:
            for t in self.transformers:
                logging.debug('Optimizer.build_grid_search_points: Grid search values for %s: %s' % (t.name, t.grid_search_values()))
            # Map min and max of each column to its corresponding real value.
            # Build linear space between min and max real values.
            # Map to non-real values.
            grid_coordinates = [t.grid_search_values() for t in self.transformers]
            # Create cross product of coordinates.
            g = np.array(list(itertools.product(*grid_coordinates)))
            # logging.debug('g=\n%s' % g)
            if len(g) > self.grid_search_max_size:
                rs = np.random.RandomState(1)
                indexes = rs.choice(len(g), size=self.grid_search_max_size, replace=False)
                g = g[indexes]
            logging.debug('Optimizer.build_grid_search_points: grid_search_points=\n%s' % g)
        else:
            g = []
        self.grid_search_points = g
        self.grid_search_size = len(g)

    def optimize_hyperparameters(self):
        if self.points_sampled:
            try:
                domain_bounds = [{'min': x[0], 'max': x[1]} for x in self.hyperparameter_domain_bounds()]
                hyperparameter_domain_info = {
                    'dim': len(domain_bounds),
                    'domain_bounds': domain_bounds,
                    }
                self.num_points_since_hyper_opt = 0.0
                logging.info('Optimizer.optimize_hyperparameters: Calling MOE to optimize hyperparameters')
                t0 = datetime.datetime.utcnow()
                response = moe_gp_hyper_opt(self.points_sampled, covariance_info=self.covariance_info, 
                    hyperparameter_domain_info=hyperparameter_domain_info, 
                    rest_host=self.moe_rest_host, rest_port=self.moe_rest_port,
                    max_num_threads=47)
                t1 = datetime.datetime.utcnow()
                td = t1 - t0
                logging.info('Optimizer.optimize_hyperparameters: MOE took %0.3f seconds and returned %s' % 
                    (time_duration_to_seconds(td), response))
                assert response['status']['optimizer_success']['log_marginal_likelihood_newton_found_update'] == True
                new_covariance_info = response['covariance_info']
                logging.debug('  old covariance_info: %s' % self.covariance_info)
                logging.debug('  new covariance_info: %s' % new_covariance_info)
                self.covariance_info = new_covariance_info
                self.gp_hyper_opt_response = response
                self.gp_hyper_opt_sec = time_duration_to_seconds(td)

                if self.observations_file:
                    self.save()
                return True
            except:
                logging.warn('Optimizer.optimize_hyperparameters: optimization failed: %s' % traceback.format_exc())
                return False

    def set_fixed_hyperparameters(self, hyperparameters_real):
        self.covariance_info = {'hyperparameters': list(hyperparameters_real), 'covariance_type': 'square_exponential'}
        self.gp_hyper_opt_response = {'covariance_info': self.covariance_info}
        self.gp_hyper_opt_sec = np.nan

    def normalize_hyperparameters(self, hyperparameters_user):
        """Convert hyperparameters in user coordinates to real values."""
        hp = list(hyperparameters_user)
        hp[0] = self.minimize_col_transformer.normalize_var(hp[0]**2)
        for i, t in enumerate(self.transformers):
            hp[i+1] = t.normalize_delta(hp[i+1])
        return hp

    def denormalize_hyperparameters(self, hyperparameters_real):
        """Convert hyperparameters in real values to user coordinates."""
        hp = list(hyperparameters_real)
        hp[0] = np.sqrt(self.minimize_col_transformer.denormalize_var(hp[0]))
        for i, t in enumerate(self.transformers):
            hp[i+1] = t.denormalize_delta(hp[i+1])            
        return hp

    def next_point_to_sample_real(self):
        optimization_info = {}

        # Determine whether we need to optimize hyperparameters or set minimize col scale.
        need_hyper_opt = False
        if self.minimize_col_transformer is None:
            # We never adjusted minimize col scale.
            need_hyper_opt = True
        elif self.num_points_since_hyper_opt >= self.max_num_points_since_hyper_opt:
            # Need to optimize hyper parameters periodically.
            need_hyper_opt = True

        # Scale minimize col.
        if need_hyper_opt:
            self.adjust_minimize_col_scale()
        
        # Now that we have a minimize col scale, we can calculate real-valued samples from already calculated samples.
        self.build_points_sampled()

        # Optimize hyperparameters.
        optimization_info['optimizer_hyperparameters_optimized'] = False
        if need_hyper_opt:
            optimization_info['optimizer_hyperparameters_optimized'] = self.optimize_hyperparameters()
        optimization_info['optimizer_gp_hyper_opt_response'] = self.gp_hyper_opt_response
        optimization_info['optimizer_gp_hyper_opt_sec'] = self.gp_hyper_opt_sec

        # Build MOE experiment object to store input
        exp = Experiment(self.domain_bounds(), points_sampled=self.points_sampled)

        # Call MOE to get next point.
        logging.info('Optimizer.next_point_to_sample_real: Calling MOE gp_next_points to get next point to sample')
        # See for optimizer defaults for DEFAULT_GRADIENT_DESCENT_PARAMETERS_EI_ANALYTIC in
        # http://yelp.github.io/MOE/_modules/moe/optimal_learning/python/constant.html
        optimizer_info = {
                "optimizer_type": "gradient_descent_optimizer", # default is gradient_descent_optimizer
                "num_multistarts": 600,         # number of locations from which to start optimization runs; default is 600
                "num_random_samples": 0,        # number of random search points to use if multistart optimization fails; default is 50000
                "optimizer_parameters": {
                    "max_num_steps": 500,       # default is 500
                    "max_num_restarts": 4,      # default is 4
                    "num_steps_averaged": 0,
                    "gamma": 0.6,               # controls decrease in step size at each iteration; default is 0.6
                    "pre_mult": 1.0e-2,         # multiplied by gradient to determine step size; default is 1.0
                    "max_relative_change": 1.0, # default is 1.0
                    "tolerance": 1.0e-7,        # default is 1.0e-7
                    },
                }
        t0 = datetime.datetime.utcnow()
        response = moe_gp_next_points(exp, covariance_info=self.covariance_info, 
            rest_host=self.moe_rest_host, rest_port=self.moe_rest_port,
            optimizer_info=optimizer_info)
        t1 = datetime.datetime.utcnow()
        td = t1 - t0
        logging.info('Optimizer.next_point_to_sample_real: MOE gp_next_points took %0.3f seconds and returned %s' % 
            (time_duration_to_seconds(td), response))
        optimization_info['optimizer_gp_next_points_response'] = response
        optimization_info['optimizer_gp_next_points_sec'] = time_duration_to_seconds(td)
        try:
            assert response['status']['optimizer_success']['gradient_descent_tensor_product_domain_found_update'] == True
        except:
            logging.warn('Optimizer.optimize_hyperparameters: optimization failed')
        pr = response["points_to_sample"][0]

        optimization_info['optimizer_point_to_sample_real'] = pr

        # Get predicted mean and variance at recommended point.
        response = self.predict_mean_var_real([pr])
        logging.debug('Optimizer.next_point_to_sample_real: predict_mean_var_real returned %s' % response)
        optimization_info['optimizer_gp_mean_var_response'] = response

        return pr, optimization_info

    def predict_mean_var_real(self, points_to_evaluate_real):
        response = moe_gp_mean_var(points_sampled=self.points_sampled, points_to_evaluate=points_to_evaluate_real, 
            covariance_info=self.covariance_info, 
            rest_host=self.moe_rest_host, rest_port=self.moe_rest_port)
        return response

    def predict_mean_var_ei(self, points_to_evaluate_df):
        points_df = pd.DataFrame([pd.Series(t.to_real_array(points_to_evaluate_df[t.name].values), name=t.name) for t in self.transformers]).T
        points_to_evaluate_real = points_df.values.tolist()
        response = moe_gp_mean_var(points_sampled=self.points_sampled, points_to_evaluate=points_to_evaluate_real, 
             covariance_info=self.covariance_info, 
             rest_host=self.moe_rest_host, rest_port=self.moe_rest_port)
        mean_real = np.array(response['mean'])
        var_real = np.array(np.matrix(response['var']).diagonal())[0]
        mean, var = self.minimize_col_transformer.denormalize_mean_var(mean_real, var_real)
        result_df = points_to_evaluate_df.copy()
        result_df['mean'] = mean
        result_df['var'] = var
        result_df['std'] = np.sqrt(var)
        result_df['ci_min'] = mean - 1.96 * result_df['std']   # 95% confidence interval
        result_df['ci_max'] = mean + 1.96 * result_df['std']   # 95% confidence interval

        response = moe_gp_ei(points_sampled=self.points_sampled, points_to_evaluate=points_to_evaluate_real, 
             covariance_info=self.covariance_info, 
             rest_host=self.moe_rest_host, rest_port=self.moe_rest_port)
        ei_real = np.array(response['expected_improvement'])
        result_df['expected_improvement_real'] = ei_real  # for troubleshooting only
        ei = self.minimize_col_transformer.denormalize_delta(ei_real)
        result_df['expected_improvement'] = ei
        return result_df

    def compare_model_to_training_data(self):
        """Diagnostic method to test optimizer."""
        points_to_evaluate = np.array([x[0] for x in self.points_sampled])
        #points_to_evaluate = points_to_evaluate + 0.01
        points_to_evaluate = points_to_evaluate.tolist()
        response = moe_gp_mean_var(points_sampled=self.points_sampled, points_to_evaluate=points_to_evaluate, covariance_info=self.covariance_info, 
            rest_host=self.moe_rest_host, rest_port=self.moe_rest_port)
        return response

    def get_samples_df(self, best_first=True):
        df = self.samples_df
        if best_first and not df.empty:
            return df.sort(['mean'])
        return df

    def get_observations_df(self, slim=True):
        df = self.observations_df
        if slim and not df.empty:
            cols = [t.name for t in self.transformers] + [self.minimize_col, 'optimizer_avoid_point']
            return df[cols]
        return df        

    def get_avoid_point_count(self):
        df = self.observations_df
        if not df.empty:
            return np.sum(df['optimizer_avoid_point'])
        return 0

    def debug_print(self):
        logging.debug('Optimizer: observations_df:\n%s' % self.get_observations_df())
        logging.debug('Optimizer: observations_df[%s].describe():\n%s' % (self.minimize_col, self.observations_df[self.minimize_col].describe()))

    def sample_similarity_score(self, criteria):
        """Given set of partial criteria, score all samples based on similarity to criteria."""
        cols = criteria.index.values
        point1 = pd.Series()
        df = self.valid_samples_df.reset_index()
        real_df = df[cols].copy()
        for col in cols:
            t = self.transformers_dict[col]
            point1[col] = t.to_real(criteria[col])
            real_df[col] = t.to_real_array(real_df[col].values)

        # Determine hyperparameters for columns in criteria
        hyperparameter_dict = {col: hp for col, hp in zip(self.transform_col_names, self.covariance_info['hyperparameters'][1:])}
        hyperparameters = [1.0] + [v for k,v in hyperparameter_dict.items() if k in cols]
        cov = SquareExponential(hyperparameters=hyperparameters)

        # Round trip means through transformer so that points to avoid are shown with means as used by the optimizer.
        df['mean_real'], _ = self.minimize_col_transformer.normalize_mean_var(df['mean'], df['var'])
        df['mean'], _ = self.minimize_col_transformer.denormalize_mean_var(df['mean_real'], df['var'])

        df['similarity'] = real_df.apply(lambda r: cov.covariance(point1.values, r[cols].values), axis=1)
        return df


class MinimizeColTransformer(object):
    """Class to scale the minimize column so that stddev is approx 1 and mean is 0."""
    def __init__(self, values=np.zeros(1), minimize=True):
        Y = values
        # logging.debug('Y=%s' % Y)
        Y = Y[np.isfinite(Y)]
        self.normalize_offset = Y.mean()
        std = Y.std()
        if std == 0.0:
            self.normalize_scale = 1.0
        else:    
            self.normalize_scale = 1.0 / std
        if not minimize:
            self.normalize_scale = -1.0 * self.normalize_scale
        self.avoid_value = 3.0    # roughly worse 99%
        self.avoid_var = 1.0e-12  # we are very confident of this
        logging.debug('MinimizeColTransformer: mean=%f, std=%s' % (self.normalize_offset, 1.0/self.normalize_scale))

    def normalize_mean_var(self, mean, var):
        """Convert unnormalized values to normalized values having 0 mean and 1 stddev.
        Infinite values that should be avoided will be reduced to finite values."""
        avoid_mask = (mean == np.inf)
        normalized_mean = (mean - self.normalize_offset) * self.normalize_scale
        normalized_mean[avoid_mask] = self.avoid_value
        normalized_var = var * (self.normalize_scale**2)
        normalized_var[avoid_mask] = self.avoid_var
        return normalized_mean, normalized_var

    def normalize_var(self, var):
        """Convert unnormalized values to normalized values having 0 mean and 1 stddev."""
        normalized_var = var * (self.normalize_scale**2)
        return normalized_var

    def denormalize_mean_var(self, normalized_mean, normalized_var):
        """Convert normalized values to denormalized values."""
        mean = normalized_mean / self.normalize_scale + self.normalize_offset
        var = normalized_var / (self.normalize_scale**2)
        return mean, var

    def denormalize_var(self, normalized_var):
        """Convert normalized values to denormalized values."""
        var = normalized_var / (self.normalize_scale**2)
        return var

    def denormalize_delta(self, normalized_delta):
        """Convert normalized delta to denormalized delta. Used for expected improvement."""
        delta = normalized_delta / self.normalize_scale
        return delta

class IdentityTransformer(object):
    """Class to transform user-space parameters to real-valued parameters. 
    Real-value parameters are between 0.0 and 1.0 (unless user-space outside of domain).
    Used to round-trip observations to real and then non-real values (no clamping in case search domain is reduced).
    Used to convert round-tripped observations to real values for MOE (no clamping in case search domain is reduced).
    Used to convert real values from MOE to non-real values for experiment to use (no clamping required since MOE output is always in domain).
    """
    def __init__(self, name, min, max, grid_search=3, fixed_value=None, data_type=None, value_if_missing=None, 
            hyperparameter_min=None, hyperparameter_max=None, **kwargs):
        self.name = name
        self.min = math_expr(min)
        self.max = math_expr(max)
        self.grid_search = grid_search
        if isinstance(self.grid_search, list):
            self.grid_search = [math_expr(x) for x in self.grid_search]
        self.fixed_value = math_expr(fixed_value)
        self.data_type = data_type
        self.value_if_missing = value_if_missing
        self.hyperparameter_min = math_expr(hyperparameter_min)
        self.hyperparameter_max = math_expr(hyperparameter_max)

        # Determine normalization values
        self.normalize_scale = 1.0
        self.normalize_offset = 0.0
        non_normalized_bounds_real = self.bounds_real()
        non_normalized_bounds_real_range = non_normalized_bounds_real[1] - non_normalized_bounds_real[0]
        self.normalize_scale = 1.0/(non_normalized_bounds_real_range)
        self.normalize_offset = non_normalized_bounds_real[0]

        logging.debug('IdentityTransformer: name=%s, normalize_scale=%f, normalize_offset=%f, bounds_real=%s' % (
            self.name, self.normalize_scale, self.normalize_offset, self.bounds_real()))

    def to_real_array(self, v):
        """Convert vector of non-real values to real values. Override for other transformers."""
        u = v.astype(np.float64)
        return self.normalize(u)

    def from_real_array(self, r):
        """Convert vector of real values to non-real values. Override for other transformers."""
        v = self.denormalize(r)
        # v = np.maximum(v, self.min)
        # v = np.minimum(v, self.max)
        v = self.as_data_type(v)
        return r
    
    def to_real(self, v):
        return self.to_real_array(np.array([v]))[0]

    def from_real(self, v):
        return self.from_real_array(np.array([v]))[0]
        
    def normalize(self, u):
        """Convert unnormalized real values to normalized values between 0.0 and 1.0."""
        r = (u - self.normalize_offset) * self.normalize_scale
        return r

    def denormalize(self, r):
        """Convert normalized real values between 0.0 and 1.0 to denormalized values."""
        u = r / self.normalize_scale + self.normalize_offset
        return u
    
    def bounds(self):
        return [self.min, self.max]

    def bounds_real(self):
        return np.sort(self.to_real_array(np.array(self.bounds()))).tolist()

    def hyperparameter_bounds(self):
        hp_real = self.hyperparameter_bounds_real()
        return self.denormalize_delta(hp_real)

    def hyperparameter_bounds_real(self):
        if self.hyperparameter_min is None:
            hyperparameter_min_real = 0.02
        else:
            hyperparameter_min_real = self.normalize_delta(self.hyperparameter_min)
        if self.hyperparameter_max is None:
            hyperparameter_max_real = 4.0
        else:
            hyperparameter_max_real = self.normalize_delta(self.hyperparameter_max)
        return np.array([hyperparameter_min_real, hyperparameter_max_real])

    def normalize_delta(self, r):
        """Convert denormalized delta (e.g. hyperparameter) to normalized delta."""
        u = r * self.normalize_scale
        return u

    def denormalize_delta(self, r):
        """Convert normalized delta (e.g. hyperparameter) to denormalized delta."""
        u = r / self.normalize_scale
        return u

    def as_data_type(self, v):
        if self.data_type:
            v = v.astype(self.data_type)        
        return v

    def grid_search_values(self):
        """Returns an array of non-real coordinate values to use for the grid search.
        grid_search == 0: no coordinates (grid search unavailable)
        grid_search == 1: use mean of min and max (in real coordinates)
        grid_search == 2: use min and max
        grid_search == 3: combination of previous two
        grid_search >= 4: linspace between min and max inclusive (in real coordinates)"""
        if isinstance(self.grid_search, int):
            if self.grid_search == 0:
                return np.array([])
            elif self.grid_search == 1:
                real_coordinates = np.array([np.mean(self.bounds_real())])
            else:
                real_coordinates = np.linspace(*self.bounds_real(), num=self.grid_search)
                # Order grid search by increasing distance from the median in real coordinates.
                middle = np.median(real_coordinates)
                s = [(x, np.abs(x - middle)) for x in real_coordinates]
                s = sorted(s, key=lambda x: (x[1],x[0]))
                real_coordinates = np.array([x[0] for x in s])
            v = self.from_real_array(real_coordinates)
            # Remove nans
            v = v[~np.isnan(v)]
            # Remove duplicates, preserving order
            _, idx = np.unique(v, return_index=True)
            v = v[np.sort(idx)]
            v = np.array(v)
            v = self.as_data_type(v)
        else:
            assert isinstance(self.grid_search,list)
            v = np.array(self.grid_search)
            # Round trip values to discretize them.
            v = self.to_real_array(v)
            assert isinstance(v,np.ndarray), '%s is %s' % (self.name, type(v))
            v = self.from_real_array(v)
            assert isinstance(v,np.ndarray), '%s is %s' % (self.name, type(v))
        return v

class LinearTransformer(IdentityTransformer):
    def __init__(self, name, multiple_of=None, **kwargs):
        self.multiple_of = math_expr(multiple_of)
        super(LinearTransformer, self).__init__(name, **kwargs)
        
    def from_real_array(self, r):
        u = self.denormalize(r)
        if self.multiple_of:
            v = np.rint(u / self.multiple_of) * self.multiple_of
        else:
            v = u
        # v = np.maximum(v, self.min)
        # v = np.minimum(v, self.max)
        v = self.as_data_type(v)
        return v

class LogarithmicTransformer(IdentityTransformer):
    """Unnormalized real value will be the exponent of the base (e.g. 3 in 10**3).
    base 2: 2,4,8,16,32
    base sqrt(10): 100,316,1000,3162,10000
    """
    def __init__(self, name, base=10.0, exponent_multiple_of=None, multiple_of=None, **kwargs):
        self.base = math_expr(base)
        self.exponent_multiple_of = math_expr(exponent_multiple_of)
        self.multiple_of = math_expr(multiple_of)
        super(LogarithmicTransformer, self).__init__(name, **kwargs)
        
    def to_real_array(self, v):
        """Convert (vector) of non-real values to real values."""
        u = np.log(v.astype(np.float64)) / np.log(self.base)
        return self.normalize(u)
    
    def from_real_array(self, r):
        """Convert (vector) of real values to non-real values."""
        # logging.debug('LogarithmicTransformer.from_real: r=%s' % str(r))
        u = self.denormalize(r)
        if self.exponent_multiple_of:
            exp = np.rint(u / self.exponent_multiple_of) * self.exponent_multiple_of
        else:
            exp = u
        #print('exp=%f' % exp)
        v = np.power(self.base, exp)
        #print('v=%f' % v)
        if self.multiple_of:
            v = np.rint(v / self.multiple_of) * self.multiple_of
        #print('v=%f' % v)
        # v = np.maximum(v, self.min)
        # v = np.minimum(v, self.max)
        v = self.as_data_type(v)
        # logging.debug('LogarithmicTransformer.from_real: v=%s' % str(v))
        return v

class BooleanTransformer(IdentityTransformer):
    def __init__(self, name, grid_search=[False,True], **kwargs):
        super(BooleanTransformer, self).__init__(name, min=False, max=True, grid_search=grid_search, **kwargs)
        
    def from_real_array(self, r):
        u = self.denormalize(r)
        v = (u >= 0.5)
        return v

def get_rule_transformer(r):
    rule_type = r.get('type', 'linear')
    transformer_map = {
        'identity': IdentityTransformer,
        'linear': LinearTransformer,
        'bool': BooleanTransformer,
        'boolean': BooleanTransformer,
        'log': LogarithmicTransformer,
        }
    transformer = transformer_map[rule_type](**r)
    return transformer

def slim_results_dict(d, max_string_len=255):
    """Returns a dict with only 'small' scalar fields that are likely to be used in optimization."""
    result = {}
    for key, value in d.iteritems():
        if isinstance(value, six.string_types) and len(value) <= max_string_len:
            result[key] = str(value)
        elif type(value).__name__ == 'bool_':
            result[key] = bool(value)
        elif type(value).__name__ in {'bool','int','int64','float','float64'}:
            result[key] = value
        elif not re.match('optimizer_.*', key) is None:
            result[key] = value
    return result        

def math_expr(f):
    """Evaluate a mathematical expression in a string.
    Note that this method is not secure so it should only be used with trusted input.
    Example: math_expr('math.sqrt(10')"""
    if isinstance(f,six.string_types):
        return eval(f, {'math': math})
    else:
        return f
