
from datetime import datetime, timedelta
try:
    import obs
    from obs import ObsClient as _ObsClient, const, util, client
except:
    obs = None


class ObsClient(_ObsClient):

    def createPostSignature(self, bucketName=None, objectKey=None, expires=300, formParams=None):
        return self._createPostSignature(bucketName, objectKey, expires, formParams, self.signature.lower() == 'v4')

    def _createPostSignature(self, bucketName=None, objectKey=None, expires=300, formParams=None, is_v4=False):
        date = datetime.utcnow()
        shortDate = date.strftime(const.SHORT_DATE_FORMAT)
        longDate = date.strftime(const.LONG_DATE_FORMAT)
        securityProvider = self._get_token()

        expires = 300 if expires is None else util.to_int(expires)
        expires = date + timedelta(seconds=expires)

        expires = expires.strftime(const.EXPIRATION_DATE_FORMAT)

        formParams = self._parse_post_params(formParams, securityProvider, is_v4,
                                             bucketName, objectKey, longDate, shortDate)

        policy = ['{"expiration":"']
        policy.append(expires)
        policy.append('", "callback":[')

        # 添加callback数据
        policy.append('{"url":"' + formParams.get('url', '') + '"},')
        policy.append('{"body":"' + formParams.get('body', '') + '"},')
        policy.append('{"body-type":"' + formParams.get('body-type', '') + '"},')
        policy.append('], "conditions":[')

        matchAnyBucket = True
        matchAnyKey = True

        conditionAllowKeys = ['acl', 'bucket', 'key', 'success_action_redirect', 'redirect', 'success_action_status']

        for key, value in formParams.items():
            if key:
                key = util.to_string(key).lower()

                if key == 'bucket':
                    matchAnyBucket = False
                elif key == 'key':
                    matchAnyKey = False

                if key not in const.ALLOWED_REQUEST_HTTP_HEADER_METADATA_NAMES \
                        and not key.startswith(self.ha._get_header_prefix()) \
                        and not key.startswith(const.OBS_HEADER_PREFIX) and key not in conditionAllowKeys:
                    continue

                policy.append('{"')
                policy.append(key)
                policy.append('":"')
                policy.append(util.to_string(value))
                policy.append('"},')

        if matchAnyBucket:
            policy.append('["starts-with", "$bucket", ""],')

        if matchAnyKey:
            policy.append('["starts-with", "$key", ""],')

        policy.append(']}')

        originPolicy = ''.join(policy)

        policy = util.base64_encode(originPolicy)

        result = self._parse_post_signature_type(is_v4, securityProvider, originPolicy,
                                                 policy, formParams, shortDate, longDate)
        return client._CreatePostSignatureResponse(**result)
