from pytest_mock.plugin import MockerFixture

from pyspark.sql import SparkSession, DataFrameReader

from app import Hoge


class TestHoge:
    def test_run(self, mocker: MockerFixture) -> None:
        hoge = Hoge()
        # mocker.patch.object(SparkSession, 'read', return_value=hoge.spark.read, new_callable=mocker.PropertyMock)
        format_mock = mocker.patch.object(DataFrameReader, 'format', return_value=hoge.spark.read)
        option_mock = mocker.patch.object(DataFrameReader, 'option', return_value=hoge.spark.read)
        load_mock = mocker.patch.object(DataFrameReader, 'load', return_value=hoge.spark.read)

        hoge.run()

        format_mock.assert_called()
        format_mock.assert_called_with('csv')
        option_mock.assert_has_calls([
            mocker.call('header', 'true'),
            mocker.call('inferScheme', 'true')
        ])
        load_mock.assert_called_with('hoge.csv')
