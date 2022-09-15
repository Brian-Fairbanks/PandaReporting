import types
from torch import nn
from ._arcgis_model import _get_backbone_meta

try:
    from fastai.callbacks.hooks import hook_outputs, model_sizes
    from fastai.torch_core import one_param
    from fastai.vision import create_body
    import timm
    import fnmatch

    HAS_FASTAI = True
except Exception as e:
    HAS_FASTAI = False


def _default_split(m):
    return (m[1],)


def _tresnet_split(m):
    return (m[0].feature.body.layer4,)


def _squeezenet_split(m):
    return (m[0][0][5], m[0][0][8], m[1])


def _densenet_split(m):
    return (m[0][0][7], m[0][0][9])


def _vgg_split(m):
    return m[0][0][15]


def _rep_vgg(m):
    return m[0][1][2]


def _mobilenetv2_split(m):
    return m[1]


def _darknet_split(m):
    return m[0][1][4]


def _cspres_split(m):
    return m[0][1][3]


def _nfnet_split(m):
    return m[0][1][-1]


def _dpn_split(m):
    return m[0][0][-2]


def _esevovnet_split(m):
    return m[0][1][-1]


def _gernet_split(m):
    return m[0][1][-2]


def _modified_cut(m):
    def forward_modified(self, img):
        return self.forward_features(img)

    m.forward = types.MethodType(forward_modified, m)

    class TimmBackbone(nn.Module):
        def __init__(self, m):
            super(TimmBackbone, self).__init__()
            self.feature = m

        def forward(self, x):
            return self.feature.forward_features(x)

    return TimmBackbone(m)


timm_model_meta = {
    "default": {"cut": None, "split": _default_split},
    "squeezenet": {"cut": -1, "split": _squeezenet_split},
    "densenet": {"cut": None, "split": _densenet_split},
    "repvgg": {"cut": -2, "split": _rep_vgg},
    "vgg": {"cut": -2, "split": _vgg_split},
    "mobilenet": {"cut": None, "split": _mobilenetv2_split},
    "darknet": {"cut": None, "split": _darknet_split},
    "hrnet": {"cut": _modified_cut, "split": _default_split},
    "nasnet": {"cut": _modified_cut, "split": _default_split},
    "selecsls": {"cut": _modified_cut, "split": _default_split},
    "tresnet": {"cut": _modified_cut, "split": _tresnet_split},
    "cspres": {"cut": None, "split": _cspres_split},
    "nfnet": {"cut": None, "split": _nfnet_split},
    "dpn": {"cut": None, "split": _dpn_split},
    "ese_vovnet": {"cut": None, "split": _esevovnet_split},
    "gernet": {"cut": None, "split": _gernet_split},
}


def timm_config(arch):
    model_name = arch if type(arch) is str else arch.__name__
    model_key = [key for key in timm_model_meta if key in model_name] + ["default"]
    return timm_model_meta.get(model_key[0])


def filter_timm_models(flt=[]):

    models = timm.list_models(pretrained=True)
    # remove transformer models
    flt = [
        "*cait*",
        "*coat*",
        "*convit*",
        "*deit*",
        "*gmixer*",
        "*gmlp*",
        "*levit*",
        "*mixer*",
        "*pit*",
        "*resmlp*",
        "*swin*",
        "*tnt*",
        "*twins*",
        "*visformer*",
        "vit_*",
    ] + flt
    flt_models = []
    for f in flt:
        flt_models.extend(fnmatch.filter(models, f))
    return sorted(set(models) - set(flt_models))


def _get_feature_size(arch, cut, chip_size=(64, 64), channel_in=3):
    m = nn.Sequential(*create_body(arch, False, cut).children())
    if "tresnet" in arch.__module__:
        with hook_outputs(m) as hooks:
            dummy_batch = (
                one_param(m)
                .new(1, channel_in, *chip_size)
                .requires_grad_(False)
                .uniform_(-1.0, 1.0)
            )
            x = m.eval()(dummy_batch)
            return [o.stored.shape for o in hooks]
    else:
        return model_sizes(m, chip_size)


def get_backbone(backbone_fn, pretrained):

    if "timm" in backbone_fn.__module__:
        backbone_cut = timm_config(backbone_fn)["cut"]
    elif getattr(backbone_fn, "_is_multispectral", False):
        backbone_cut = _get_backbone_meta(backbone_fn.__name__)["cut"]
    else:
        backbone_cut = None

    return create_body(backbone_fn, pretrained, backbone_cut)
