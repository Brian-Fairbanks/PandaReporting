from ._machine_learning import MLModel, raise_data_exception
import os
import shutil
import tempfile
import random
import json
import pickle
import warnings
import math
import shutil
import os
import time
from pathlib import Path
import traceback
import arcgis
from arcgis.features import FeatureLayer

HAS_AUTO_ML_DEPS = True
import_exception = None

try:
    from ._arcgis_model import ArcGISModel, _raise_fastai_import_error
    from arcgis.learn._utils.tabular_data import TabularDataObject, add_h3
    from arcgis.learn._utils.common import _get_emd_path
    from arcgis.learn._utils.utils import arcpy_localization_helper

    HAS_FASTAI = True
except:
    import_exception = traceback.format_exc()
    HAS_FASTAI = False

try:
    import sklearn
    from sklearn import *
    from sklearn import preprocessing
    import numpy as np
    import pandas as pd
except Exception as e:
    import_exception = "\n".join(
        traceback.format_exception(type(e), e, e.__traceback__)
    )
    HAS_AUTO_ML_DEPS = False

HAS_FAST_PROGRESS = True
try:
    from fastprogress.fastprogress import progress_bar
except Exception as e:
    import_exception = "\n".join(
        traceback.format_exception(type(e), e, e.__traceback__)
    )
    HAS_FAST_PROGRESS = False

_PROTOCOL_LEVEL = 2


class AutoML(object):
    """
    Automates the process of model selection, training and hyperparameter tuning of
    machine learning models within a specified time limit. Based upon
    MLJar(https://github.com/mljar/mljar-supervised/) and scikit-learn.

    Note that automated machine learning support is provided only for supervised learning.
    Refer https://supervised.mljar.com/

    =====================   ===========================================
    **Argument**            **Description**
    ---------------------   -------------------------------------------
    data                    Required TabularDataObject. Returned data object from
                            `prepare_tabulardata` function.
    ---------------------   -------------------------------------------
    total_time_limit        Optional Int. The total time limit in seconds for
                            AutoML training.
                            Default is 3600 (1 Hr)
    ---------------------   -------------------------------------------
    mode                    Optional Str.
                            Can be {Basic, Intermediate, Advanced}. This parameter defines
                            the goal of AutoML and how intensive the AutoML search will be.

                            Basic : To to be used when the user wants to explain and
                                      understand the data.
                                      Uses 75%/25% train/test split.
                                      Uses the following models: Baseline, Linear, Decision Tree,
                                      Random Forest, XGBoost, Neural Network, and Ensemble.
                                      Has full explanations in reports: learning curves, importance
                                      plots, and SHAP plots.
                            Intermediate : To be used when the user wants to train a model that will be
                                      used in real-life use cases.
                                      Uses 5-fold CV (Cross-Validation).
                                      Uses the following models: Linear, Random Forest, LightGBM,
                                      XGBoost, CatBoost, Neural Network, and Ensemble.
                                      Has learning curves and importance plots in reports.
                            Advanced : To be used for machine learning competitions (maximum performance).
                                      Uses 10-fold CV (Cross-Validation).
                                      Uses the following models: Decision Tree, Random Forest, Extra Trees,
                                      XGBoost, CatBoost, Neural Network, Nearest Neighbors, Ensemble,
                                      and Stacking.It has only learning curves in the reports.
                                      Default is Basic.
    ---------------------   -------------------------------------------
    algorithms              Optional. List of str.
                            The list of algorithms that will be used in the training. The algorithms can be:
                            Linear, Decision Tree, Random Forest, Extra Trees, LightGBM, Xgboost, Neural Network
    ---------------------   -------------------------------------------
    eval_metric             Optional  Str. The metric to be used to compare models.
                            Possible values are:
                            For binary classification - logloss (default), auc, f1, average_precision,
                            accuracy.
                            For multiclass classification - logloss (default), f1, accuracy
                            For regression - rmse (default), mse, mae, r2, mape, spearman, pearson

                            Note - If there are only 2 unique values in the target, then
                            binary classification is performed,
                            If number of unique values in the target is between 2 and 20 (included), then
                            multiclass classification is performed,
                            In all other cases, regression is performed on the dataset.
    ---------------------   -------------------------------------------
    n_jobs                  Optional. Int.
                            Number of CPU cores to be used. By default, it is set to 1.Set it
                            to -1 to use all the cores.
    =====================   ===========================================

    :return: `AutoML` Object
    """

    def __init__(
        self,
        data=None,
        total_time_limit=3600,
        mode="Basic",
        algorithms=None,
        eval_metric="auto",
        n_jobs=1,
        ml_task="auto",
    ):
        try:
            from supervised.automl import AutoML as base_AutoML
        except Exception as e:
            import_exception = "\n".join(
                traceback.format_exception(type(e), e, e.__traceback__)
            )
            _raise_fastai_import_error(import_exception=import_exception)

        if not HAS_AUTO_ML_DEPS:
            _raise_fastai_import_error(import_exception=import_exception)

        self._data = data
        if getattr(self._data, "_is_unsupervised", False):
            raise Exception(
                "Auto ML feature is currently only available for Supervised learning."
            )
        if getattr(self._data, "_is_not_empty", False):
            if (len(data._training_indexes) < 20) & (
                eval_metric in ["r2", "rmse", "mse", "mape", "spearman", "pearson"]
            ):
                warnings.warn(
                    "The eval metric you have passed, is not valid for a classification usecase. If the use case is regression, then ensure that your dataset has atleast 22 records"
                )
                return

        if algorithms:
            algorithms = algorithms
        else:
            algorithms = [
                "Linear",
                "Decision Tree",
                "Random Forest",
                "Extra Trees",
                "LightGBM",
                "Xgboost",
                "Neural Network",
            ]

        if getattr(self._data, "_is_not_empty", True):
            (
                self._training_data,
                self._training_labels,
                self._validation_data,
                self._validation_labels,
            ) = self._data._ml_data
            self._all_data = np.concatenate(
                (self._training_data, self._validation_data), axis=0
            )
            self._all_labels = np.concatenate(
                (self._training_labels, self._validation_labels), axis=0
            )
            self._validation_data_df = pd.DataFrame(
                self._validation_data,
                columns=self._data._continuous_variables
                + self._data._categorical_variables,
            )
            self._all_data_df = pd.DataFrame(
                self._all_data,
                columns=self._data._continuous_variables
                + self._data._categorical_variables,
            )
            if ml_task == "auto":
                ml_task = self.get_ml_task(self._all_labels)
            if ml_task == "text":
                msg = arcpy_localization_helper(
                    "Dependent variable has more than 200 unique values more than half of the total records are unique, hence there is not enough information to train a model",
                    260154,
                    "ERROR",
                )
                exit(260146)
            if (mode == "Explain") or (mode == "Basic"):
                explain_level = 2
                zone_list = ["zone3_id", "zone4_id", "zone5_id", "zone6_id", "zone7_id"]
                # try:
                #    self._all_data_df = self._all_data_df.drop(columns=zone_list)
                #    self._data._continuous_variables = [x for x in self._data._continuous_variables if x not in zone_list]
                #    self._data._categorical_variables = [x for x in self._data._categorical_variables if x not in zone_list]
                # except:
                #    pass
            else:
                explain_level = 0  # Setting explain level to 0 in case of Perform and Compete as EDA seems to be creating memory issues

            # Mapping and conversion of old mode names to new
            api_modes = ["Explain", "Perform", "Compete"]
            tool_modes = ["Basic", "Intermediate", "Advanced"]
            if mode in tool_modes:
                mode = api_modes[tool_modes.index(mode)]
            elif mode in api_modes:
                mode = mode
            else:
                mode = "Explain"

            try:
                import arcpy

                result_path = tempfile.mkdtemp(dir=arcpy.env.scratchFolder)
            except:
                result_path = tempfile.mkdtemp(dir=tempfile.gettempdir())

            self._model = base_AutoML(
                results_path=result_path,
                mode=mode,
                ml_task=ml_task,
                algorithms=algorithms,
                total_time_limit=total_time_limit,
                golden_features=False,
                explain_level=explain_level,
                eval_metric=eval_metric,
                n_jobs=n_jobs,
                kmeans_features=False,
            )
        else:
            result_path = self._data.path
            self._model = base_AutoML(results_path=result_path)
            self._model._results_path = self._data.path

    def get_ml_task(self, all_labels):
        try:
            if isinstance(all_labels[0], str):
                unique = np.unique(all_labels, return_counts=False)
                if len(unique) == 2:
                    return "binary_classification"
                elif len(unique) > 200 and len(unique) > int(0.5 * all_labels.shape[0]):
                    return "text"
                else:
                    return "multiclass_classification"
            else:
                return "auto"
        except:
            return "auto"

    def fit(self):
        """
        Fits the AutoML model.
        """
        if getattr(self._data, "_is_not_empty", True):
            try:
                self._model.fit(self._all_data_df, self._all_labels)
            except:
                msg = arcpy_localization_helper(
                    "The desired models could not be trained using the input data provided.",
                    260150,
                    "ERROR",
                )
                exit()
        else:
            raise Exception("Fit can be called only with data.")
        # self.save()
        print(
            "All the evaluated models are saved in the path ",
            os.path.abspath(self._model._get_results_path()),
        )

    def show_results(self, rows=5):
        """
        Shows sample results for the model.

        =====================   ===========================================
        **Argument**            **Description**
        ---------------------   -------------------------------------------
        rows                    Optional number of rows. By default, 5 rows
                                are displayed.
        =====================   ===========================================
        :returns dataframe
        """
        if getattr(self._data, "is_not_empty", True) == False:
            raise Exception(
                "This method is not available when the model is initiated for prediction"
            )
        if (
            not self._data._is_unsupervised
            and (self._validation_data is None or self._validation_labels is None)
        ) or (self._data._is_unsupervised and self._validation_data is None):
            raise_data_exception()

        min_size = len(self._validation_data)

        if rows < min_size:
            min_size = rows

        # sample_batch = random.sample(self._data._validation_indexes, min_size)
        sample_batch = random.sample(range(len(self._validation_data)), min_size)
        validation_data_batch = self._validation_data.take(sample_batch, axis=0)
        # validation_data_batch_df = pd.DataFrame(validation_data_batch,
        # columns=self._data._continuous_variables + self._data._categorical_variables)
        sample_indexes = [self._data._validation_indexes[i] for i in sample_batch]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            output_labels = self._predict(validation_data_batch)
        pd.options.mode.chained_assignment = None
        df = self._data._dataframe.iloc[
            sample_indexes
        ]  # .loc[sample_batch]#.reset_index(drop=True).loc[sample_batch].reset_index(drop=True)

        if self._data._dependent_variable:
            df[self._data._dependent_variable + "_results"] = output_labels
        else:
            df["prediction_results"] = output_labels

        return df.sort_index()

    def score(self):
        """
        :returns output from AutoML's model.score(), R2 score in case of regression and Accuracy in case of classification.
        """
        if getattr(self._data, "_is_not_empty", True):
            return self._model.score(self._validation_data_df, self._validation_labels)
        else:
            raise Exception(
                "This method is not available when the model is initiated for prediction"
            )

    def report(self):
        """
        :returns a report of the different models trained by AutoML along with their performance.
        """
        main_readme_html = os.path.join(self._model._results_path, "README.html")
        warnings.warn(
            "In case the report html is not rendered appropriately in the notebook, the same can be found in the path "
            "" + main_readme_html
        )
        return self._model.report()

    def predict_proba(self):
        """
        :returns output from AutoML's model.predict_proba() with prediction probability for the training data
        """
        if (self._data._is_classification == "classification") or (
            self._data._is_classification == True
        ):
            if getattr(self._data, "_is_not_empty", False):
                raise Exception(
                    "This method is not available when the model is initiated for prediction"
                )
            else:
                cols = (
                    self._data._continuous_variables + self._data._categorical_variables
                )
                data_df = pd.DataFrame(self._data._ml_data[0], columns=cols)
                return self._model.predict_proba(data_df)
        else:
            raise Exception("This method is applicable only for classification models.")

    def copy_and_overwrite(self, from_path, to_path):
        dest_dir = os.path.join(to_path, os.path.basename(from_path))
        if os.path.exists(dest_dir):
            shutil.rmtree(dest_dir)
        shutil.copytree(from_path, dest_dir)

    def _copy_reports(self, src_dir, dest):
        extensions = [".html", ".png", ".svg"]

        for root, dirs, files in os.walk(src_dir):
            for file in files:
                for extension in extensions:
                    if file.endswith(extension):
                        folder_name = os.path.basename(root)
                        src = os.path.join(root, file)
                        dest_folder = dest
                        if not folder_name.startswith("tmp"):
                            dest_folder = os.path.join(dest, folder_name)
                            if not os.path.isdir(dest_folder):
                                os.mkdir(dest_folder)
                        dest_folder = os.path.join(dest_folder, file)
                        shutil.copy2(src, dest_folder)

    def save(self, path):
        """
        Saves the model in the path specified. Creates an Esri Model and a dlpk.
        Uses pickle to save the model and transforms.

        =====================   ===========================================
        **Argument**            **Description**
        ---------------------   -------------------------------------------
        path                    Path of the directory where the model should be saved.
        =====================   ===========================================
        :returns path
        """
        if getattr(self._data, "_is_not_empty", True) == False:
            raise Exception(
                "This method is not available when the model is initiated for prediction"
            )
        # Required files to be copied to new path
        files_required = [
            "data_info.json",
            "ldb_performance.png",
            "ldb_performance_boxplot.png",
            "params.json",
            "progress.json",
            "README.md",
            "drop_features.json",
        ]
        required_model_folders = []  # List of folders that are to be copied to new path
        base_file_name = os.path.basename(self._model._get_results_path())
        result_path = os.path.abspath(self._model._get_results_path())

        save_model_path = os.path.abspath(path)
        if not os.path.exists(save_model_path):
            os.makedirs(save_model_path)

        MLModel._save_encoders(
            self._data._encoder_mapping, save_model_path, base_file_name
        )

        if self._data._procs:
            MLModel._save_transforms(self._data._procs, save_model_path, base_file_name)

        self._write_emd(save_model_path, base_file_name)
        if (self._model._best_model._name == "Ensemble") or (
            self._model._best_model._name == "Ensemble_Stacked"
        ):
            model_map = self._model._best_model.models_map
            required_model_folders.append(os.path.join(result_path, "Ensemble"))
            for i in self._model._best_model.selected_models:
                # print(i['model'])
                sub_path = list(model_map.keys())[
                    list(model_map.values()).index(i["model"])
                ]
                final_path = os.path.join(result_path, sub_path)
                required_model_folders.append(final_path)
        else:
            final_path = os.path.join(result_path, self._model._best_model._name)
            required_model_folders.append(final_path)

        for folder in required_model_folders:
            # copyfolder(folder,dest)
            self.copy_and_overwrite(folder, save_model_path)

        for file in files_required:
            abs_file_path = os.path.join(result_path, file)
            dest_file = os.path.join(save_model_path, os.path.basename(file))
            if os.path.isfile(abs_file_path):
                shutil.copyfile(abs_file_path, dest_file)

        # Copies reports if present
        try:
            self._copy_reports(result_path, save_model_path)
            copy_success = True
        except:
            copy_success = False
        if copy_success:
            shutil.rmtree(result_path)
        # Creates dlpk
        from ._arcgis_model import _create_zip

        _create_zip(Path(save_model_path).name, str(save_model_path))

        print("Model has been saved in the path", save_model_path)
        return save_model_path

    def _write_emd(self, path, base_file_name):
        emd_file = os.path.join(path, base_file_name + ".emd")
        emd_params = {}
        emd_params["version"] = str(sklearn.__version__)
        # if not self._data._is_unsupervised:
        emd_params["score"] = self.score()
        emd_params["_is_classification"] = (
            "classification" if self._data._is_classification else "regression"
        )
        emd_params["ModelName"] = "AutoML"
        emd_params["ResultsPath"] = self._model._results_path
        # emd_params['ModelFile'] = base_file_name + '.pkl'
        # emd_params['ModelParameters'] = self._model.get_params()
        emd_params["categorical_variables"] = self._data._categorical_variables

        if self._data._dependent_variable:
            emd_params["dependent_variable"] = self._data._dependent_variable

        emd_params["continuous_variables"] = self._data._continuous_variables
        if self._data._feature_field_variables:
            emd_params["_feature_field_variables"] = self._data._feature_field_variables
        if self._data._raster_field_variables:
            emd_params["_raster_field_variables"] = self._data._raster_field_variables

        with open(emd_file, "w") as f:
            f.write(json.dumps(emd_params, indent=4))

    @classmethod
    def from_model(cls, emd_path):
        """
        Creates a `MLModel` Object from an Esri Model Definition (EMD) file.

        =====================   ===========================================
        **Argument**            **Description**
        ---------------------   -------------------------------------------
        emd_path                Required string. Path to Esri Model Definition
                                file.
        =====================   ===========================================

        :return: `AutoML` Object
        """
        if not HAS_FASTAI:
            _raise_fastai_import_error(import_exception=import_exception)
        emd_path = _get_emd_path(emd_path)
        if not HAS_AUTO_ML_DEPS:
            _raise_fastai_import_error(import_exception=import_exception)

        if not os.path.exists(emd_path):
            raise Exception("Invalid data path.")

        with open(emd_path, "r") as f:
            emd = json.loads(f.read())

        categorical_variables = emd["categorical_variables"]
        dependent_variable = emd.get("dependent_variable", None)
        continuous_variables = emd["continuous_variables"]

        if emd["version"] != str(sklearn.__version__):
            warnings.warn(
                f"Sklearn version has changed. Model Trained using version {emd['version']}"
            )

        _is_classification = True
        if emd["_is_classification"] != "classification":
            _is_classification = False

        encoder_mapping = None
        if categorical_variables:
            encoder_path = os.path.join(
                os.path.dirname(emd_path),
                os.path.basename(emd_path).split(".")[0] + "_encoders.pkl",
            )
            if os.path.exists(encoder_path):
                with open(encoder_path, "rb") as f:
                    encoder_mapping = pickle.loads(f.read())

        column_transformer = None
        transforms_path = os.path.join(
            os.path.dirname(emd_path),
            os.path.basename(emd_path).split(".")[0] + "_transforms.pkl",
        )
        if os.path.exists(transforms_path):
            with open(transforms_path, "rb") as f:
                column_transformer = pickle.loads(f.read())

        empty_data = TabularDataObject._empty(
            categorical_variables,
            continuous_variables,
            dependent_variable,
            encoder_mapping,
            column_transformer,
        )
        empty_data._is_classification = _is_classification
        empty_data._is_not_empty = False
        # empty_data.path = emd["ResultsPath"]
        empty_data.path = emd_path.parent
        return cls(data=empty_data)

    def _predict(self, data):
        data_df = pd.DataFrame(
            data,
            columns=self._data._continuous_variables
            + self._data._categorical_variables,
        )
        return self._model.predict(data_df)

    def predict(
        self,
        input_features=None,
        explanatory_rasters=None,
        datefield=None,
        distance_features=None,
        output_layer_name="Prediction Layer",
        gis=None,
        prediction_type="features",
        output_raster_path=None,
        match_field_names=None,
        cell_sizes=[3, 4, 5, 6, 7],
    ):
        """

        Predict on data from feature layer, dataframe and or raster data.

        =================================   =========================================================================
        **Argument**                        **Description**
        ---------------------------------   -------------------------------------------------------------------------
        input_features                      Optional Feature Layer or spatial dataframe. Required if prediction_type='features'.
                                            Contains features with location and
                                            some or all fields required to infer the dependent variable value.
        ---------------------------------   -------------------------------------------------------------------------
        explanatory_rasters                 Optional list. Required if prediction_type='raster'.
                                            Contains a list of raster objects containing
                                            some or all fields required to infer the dependent variable value.
        ---------------------------------   -------------------------------------------------------------------------
        datefield                           Optional string. Field name from feature layer
                                            that contains the date, time for the input features.
                                            Same as `prepare_tabulardata()`.
        ---------------------------------   -------------------------------------------------------------------------
        cell_sizes                          Size of H3 cells (specified as H3 resolution) for spatially
                                            aggregating input features and passing in the cell ids as additional
                                            explanatory variables to the model. If a spatial dataframe is passed
                                            as input_features, ensure that the spatial reference is 4326,
                                            and the geometry type is Point. Not applicable when explanatory_rasters
                                            are provided.
        ---------------------------------   -------------------------------------------------------------------------
        distance_features                   Optional List of Feature Layer objects.
                                            These layers are used for calculation of field "NEAR_DIST_1",
                                            "NEAR_DIST_2" etc in the output dataframe.
                                            These fields contain the nearest feature distance
                                            from the input_features.
                                            Same as `prepare_tabulardata()`.
        ---------------------------------   -------------------------------------------------------------------------
        output_layer_name                   Optional string. Used for publishing the output layer.
        ---------------------------------   -------------------------------------------------------------------------
        gis                                 Optional GIS Object. Used for publishing the item.
                                            If not specified then active gis user is taken.
        ---------------------------------   -------------------------------------------------------------------------
        prediction_type                     Optional String.
                                            Set 'features' or 'dataframe' to make output feature layer predictions.
                                            With this feature_layer argument is required.

                                            Set 'raster', to make prediction raster.
                                            With this rasters must be specified.
        ---------------------------------   -------------------------------------------------------------------------
        output_raster_path                  Optional path.
                                            Required when prediction_type='raster', saves
                                            the output raster to this path.
        ---------------------------------   -------------------------------------------------------------------------
        match_field_names                   Optional dictionary.
                                            Specify mapping of field names from prediction set
                                            to training set.
                                            For example:
                                                {
                                                    "Field_Name_1": "Field_1",
                                                    "Field_Name_2": "Field_2"
                                                }
        =================================   =========================================================================

        :returns Feature Layer if prediction_type='features', dataframe for prediction_type='dataframe' else creates an output raster.

        """

        rasters = explanatory_rasters if explanatory_rasters else []
        if prediction_type in ["features", "dataframe"]:

            if input_features is None:
                raise Exception("Feature Layer required for predict_features=True")

            gis = gis if gis else arcgis.env.active_gis
            return self._predict_features(
                input_features,
                rasters,
                datefield,
                cell_sizes,
                distance_features,
                output_layer_name,
                gis,
                match_field_names,
                prediction_type,
            )
        else:
            if not rasters:
                raise Exception("Rasters required for predict_features=False")

            if not output_raster_path:
                raise Exception(
                    "Please specify output_raster_folder_path to save the output."
                )

            return self._predict_rasters(output_raster_path, rasters, match_field_names)

    def _predict_features(
        self,
        input_features,
        rasters=None,
        datefield=None,
        cell_sizes=[3, 4, 5, 6, 7],
        distance_feature_layers=None,
        output_name="Prediction Layer",
        gis=None,
        match_field_names=None,
        prediction_type="features",
    ):
        dataframe_complete = False
        if isinstance(input_features, FeatureLayer):
            if cell_sizes and not rasters:
                dataframe = input_features.query(out_sr=4326).sdf
                dataframe = add_h3(dataframe, cell_sizes)
            else:
                dataframe = input_features.query().sdf
        elif (
            hasattr(input_features, "dataSource")
            or str(input_features).endswith(".shp")
            or isinstance(input_features, tuple)
        ):
            dataframe, index_data = TabularDataObject._sdf_gptool_workflow(
                input_features,
                distance_feature_layers,
                rasters,
                index_field=None,
                is_table_obj=False,
            )
            if cell_sizes and not rasters:
                dataframe = add_h3(dataframe, cell_sizes)
            dataframe_complete = True
        elif hasattr(input_features, "value"):
            dataframe, index_data = TabularDataObject._sdf_gptool_workflow(
                input_features,
                distance_feature_layers,
                rasters,
                index_field=None,
                is_table_obj=True,
            )
            dataframe_complete = True
        else:
            dataframe = input_features.copy()

        fields_needed = (
            self._data._categorical_variables + self._data._continuous_variables
        )
        distance_feature_layers = (
            distance_feature_layers if distance_feature_layers else []
        )
        continuous_variables = self._data._continuous_variables

        columns = dataframe.columns
        if dataframe_complete:
            processed_dataframe = dataframe
        else:
            feature_layer_columns = []
            for column in columns:
                column_name = column
                categorical = False

                if column_name in fields_needed:
                    if column_name not in continuous_variables:
                        categorical = True
                elif match_field_names and match_field_names.get(column_name):
                    if match_field_names.get(column_name) not in continuous_variables:
                        categorical = True
                else:
                    continue

                feature_layer_columns.append((column_name, categorical))

            raster_columns = []
            if rasters:
                for raster in rasters:
                    column_name = raster.name
                    categorical = False
                    if column_name in fields_needed:
                        if column_name not in continuous_variables:
                            categorical = True
                    elif match_field_names and match_field_names.get(column_name):
                        column_name = match_field_names.get(column_name)
                        if column_name not in continuous_variables:
                            categorical = True
                    else:
                        continue

                    raster_columns.append((raster, categorical))

            with warnings.catch_warnings():
                if not HAS_FASTAI:
                    _raise_fastai_import_error(import_exception=import_exception)
                warnings.simplefilter("ignore", UserWarning)
                (
                    processed_dataframe,
                    fields_mapping,
                ) = TabularDataObject._prepare_dataframe_from_features(
                    input_features,
                    self._data._dependent_variable,
                    feature_layer_columns,
                    raster_columns,
                    datefield,
                    cell_sizes,
                    distance_feature_layers,
                )

        if match_field_names:
            try:
                list_of_train_fields = []
                for key, value in match_field_names.items():
                    if (not key == value) and (len(str(key)) > 0):
                        list_of_train_fields.append(value)
                processed_dataframe = processed_dataframe.drop(
                    list_of_train_fields, axis=1, errors="ignore"
                )
            except:
                pass
            processed_dataframe.rename(columns=match_field_names, inplace=True)
        for field in fields_needed:
            if field not in processed_dataframe.columns:
                msg = arcpy_localization_helper(
                    "Data on which prediction in needed does not have the fields the model was trained on",
                    260152,
                    "ERROR",
                )
                exit()

        for column in processed_dataframe.columns:
            if column not in fields_needed:
                processed_dataframe = processed_dataframe.drop(column, axis=1)

        processed_numpy = self._data._process_data(
            processed_dataframe.reindex(sorted(processed_dataframe.columns), axis=1),
            fit=False,
        )
        predictions = self._predict(processed_numpy)
        dataframe["prediction_results"] = predictions

        if prediction_type == "dataframe":
            return dataframe

        return dataframe.spatial.to_featurelayer(output_name, gis)

    def _raster_sr(self, raster):
        try:
            return raster._engine_obj._raster.spatialReference
        except:
            try:
                import arcpy

                return arcpy.SpatialReference(raster.extent["spatialReference"]["wkid"])
            except:
                try:
                    import arcpy

                    return arcpy.SpatialReference(
                        raster.extent["spatialReference"]["wkt"]
                    )
                except:
                    msg = arcpy_localization_helper(
                        "One or more input rasters do not have a valid spatial reference.",
                        517,
                        "ERROR",
                    )

    def _predict_rasters(self, output_folder_path, rasters, match_field_names=None):

        if not os.path.exists(os.path.dirname(output_folder_path)):
            raise Exception("Output directory doesn't exist")

        if os.path.exists(output_folder_path):
            raise Exception("Output Folder already exists")

        try:
            import arcpy
        except:
            raise Exception("This function requires arcpy.")

        try:
            import numpy as np
        except:
            raise Exception("This function requires numpy.")

        try:
            import pandas as pd
        except:
            raise Exception("This function requires pandas.")

        if not HAS_FAST_PROGRESS:
            raise Exception("This function requires fastprogress.")

        fields_needed = (
            self._data._categorical_variables + self._data._continuous_variables
        )

        cached_sr = arcpy.env.outputCoordinateSystem
        arcpy.env.outputCoordinateSystem = self._raster_sr(rasters[0])

        xmin = rasters[0].extent["xmin"]
        xmax = rasters[0].extent["xmax"]
        ymin = rasters[0].extent["ymin"]
        ymax = rasters[0].extent["ymax"]
        min_cell_size_x = rasters[0].mean_cell_width
        min_cell_size_y = rasters[0].mean_cell_height

        default_sr = self._raster_sr(rasters[0])

        for raster in rasters:
            point_upper_left = arcpy.PointGeometry(
                arcpy.Point(raster.extent["xmin"], raster.extent["ymax"]),
                self._raster_sr(raster),
            ).projectAs(default_sr)

            point_lower_right = arcpy.PointGeometry(
                arcpy.Point(raster.extent["xmax"], raster.extent["ymin"]),
                self._raster_sr(raster),
            ).projectAs(default_sr)

            cell_extent = arcpy.Extent(
                raster.extent["xmin"],
                raster.extent["ymin"],
                raster.extent["xmin"] + raster.mean_cell_width,
                raster.extent["ymin"] + raster.mean_cell_height,
                spatial_reference=self._raster_sr(raster),
            ).projectAs(default_sr)

            cx, cy = (
                abs(cell_extent.XMax - cell_extent.XMin),
                abs(cell_extent.YMax - cell_extent.YMin),
            )

            if xmin > point_upper_left.firstPoint.X:
                xmin = point_upper_left.firstPoint.X
            if ymax < point_upper_left.firstPoint.Y:
                ymax = point_upper_left.firstPoint.Y
            if xmax < point_lower_right.firstPoint.X:
                xmax = point_lower_right.firstPoint.X
            if ymin > point_lower_right.firstPoint.Y:
                ymin = point_lower_right.firstPoint.Y

            if min_cell_size_x < cx:
                min_cell_size_x = cx

            if min_cell_size_y < cy:
                min_cell_size_y = cy

        max_raster_columns = int(abs(math.ceil((xmax - xmin) / min_cell_size_x)))
        max_raster_rows = int(abs(math.ceil((ymax - ymin) / min_cell_size_y)))
        point_upper = arcpy.PointGeometry(arcpy.Point(xmin, ymax), default_sr)
        point_lower = arcpy.PointGeometry(arcpy.Point(xmax, ymin), default_sr)

        cell_extent = arcpy.Extent(
            xmin,
            ymin,
            xmin + min_cell_size_x,
            ymin + min_cell_size_y,
            spatial_reference=default_sr,
        )

        raster_data = {}
        for raster in rasters:
            field_name = raster.name

            point_upper_translated = point_upper.projectAs(self._raster_sr(raster))
            cell_extent_translated = cell_extent.projectAs(self._raster_sr(raster))

            if field_name not in fields_needed:
                if match_field_names and match_field_names.get(raster.name):
                    field_name = match_field_names.get(raster.name)

            ccxx, ccyy = (
                abs(cell_extent_translated.XMax - cell_extent_translated.XMin),
                abs(cell_extent_translated.YMax - cell_extent_translated.YMin),
            )

            raster_read = raster.read(
                origin_coordinate=(
                    point_upper_translated.firstPoint.X,
                    point_upper_translated.firstPoint.Y,
                ),
                ncols=max_raster_columns,
                nrows=max_raster_rows,
                cell_size=(ccxx, ccyy),
            )

            for row in range(max_raster_rows):
                for column in range(max_raster_columns):
                    values = raster_read[row][column]
                    index = 0
                    for value in values:
                        key = field_name
                        if index != 0:
                            key = key + f"_{index}"
                        if not raster_data.get(key):
                            raster_data[key] = []
                        index = index + 1
                        raster_data[key].append(value)

        for field in fields_needed:
            if (field not in list(raster_data.keys())) and (
                match_field_names and match_field_names.get(field, None) is None
            ):
                msg = arcpy_localization_helper(
                    "Data on which prediction is needed does not have the fields the model was trained on",
                    260152,
                    "ERROR",
                )
                exit()

        processed_data = []

        length_values = len(raster_data[list(raster_data.keys())[0]])
        for i in range(length_values):
            processed_row = []
            for raster_name in sorted(raster_data.keys()):
                processed_row.append(raster_data[raster_name][i])
            processed_data.append(processed_row)

        processed_df = pd.DataFrame(
            data=np.array(processed_data), columns=sorted(raster_data)
        )

        processed_numpy = self._data._process_data(processed_df, fit=False)

        predictions = self._predict(processed_numpy)

        if isinstance(predictions[0], str):
            processed_df["predictions"] = predictions
            le = preprocessing.LabelEncoder()
            le.fit(processed_df["predictions"])
            le_name_mapping = dict(zip(le.classes_, le.transform(le.classes_)))
            processed_df["predictions"] = le.transform(processed_df["predictions"])

            predictions = np.array(
                processed_df["predictions"].values.reshape(
                    [max_raster_rows, max_raster_columns]
                ),
                dtype=np.uint8,
            )
        else:
            predictions = np.array(
                predictions.reshape([max_raster_rows, max_raster_columns]),
                dtype="float64",
            )

        processed_raster = arcpy.NumPyArrayToRaster(
            predictions,
            arcpy.Point(xmin, ymin),
            x_cell_size=min_cell_size_x,
            y_cell_size=min_cell_size_y,
        )
        if isinstance(predictions[0], str):
            arcpy.management.BuildRasterAttributeTable(processed_raster)
            class_map = le_name_mapping
            "class_map=" + str(class_map)
            arcpy.management.CalculateField(
                processed_raster,
                "Class",
                expression="class_map.get(!Value!)",
                expression_type="PYTHON3",
                code_block="class_map=" + str(class_map),
            )
        processed_raster.save(output_folder_path)
        arcpy.env.outputCoordinateSystem = cached_sr

        return True
