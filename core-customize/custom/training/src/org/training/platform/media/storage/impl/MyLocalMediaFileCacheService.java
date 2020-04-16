package org.training.platform.media.storage.impl;


import de.hybris.platform.media.storage.MediaMetaData;
import de.hybris.platform.media.storage.MediaStorageConfigService;
import de.hybris.platform.media.storage.MediaStorageConfigService.MediaFolderConfig;
import de.hybris.platform.media.storage.MediaStorageRegistry;
import de.hybris.platform.media.storage.impl.DefaultLocalMediaFileCacheService;
import de.hybris.platform.media.storage.impl.DefaultMediaStorageConfigService.DefaultSettingKeys;
import de.hybris.platform.media.storage.impl.LocalFileMediaStorageStrategy;
import de.hybris.platform.media.storage.impl.MediaCacheRecreator;
import de.hybris.platform.media.storage.impl.MediaCacheRegion;
import de.hybris.platform.media.storage.impl.StoredMediaData;
import de.hybris.platform.regioncache.CacheController;
import de.hybris.platform.regioncache.CacheValueLoader;
import de.hybris.platform.regioncache.key.CacheKey;
import de.hybris.platform.util.MediaUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;


/**
 * <p>
 * Default implementation of <code>LocalMediaFileCache</code> interface. Allows to cache locally any stream and returns
 * binary data as <code>File</code> or <code>FileInputStream</code> from local cache.
 * </p>
 * <p>
 * To configure folder to use local cache use following property key:
 * <p>
 *
 * <pre>
 * folder.folderQualifier.local.cache = true
 * </pre>
 */
public class MyLocalMediaFileCacheService extends DefaultLocalMediaFileCacheService
{
	private static final Logger LOG = Logger.getLogger(DefaultLocalMediaFileCacheService.class);
	private static final String DEFAULT_CACHE_FOLDER = "cache";
	private static final int GET_RESOURCE_MAX_RETRIES = 5;
	public static final String CACHE_FILE_NAME_DELIM = "__H__";

	private MediaCacheRecreator cacheRecreator;
	private MediaStorageRegistry storageRegistry;
	private LocalFileMediaStorageStrategy storageStrategy;
	private MediaStorageConfigService storageConfigService;
	private CacheController cacheController;
	private MediaCacheRegion mediaCacheRegion;
	private File mainDataDir;

	private String tenantId;



	//-----------------------------------------
	@Override
	public File storeOrGetAsFile(final MediaFolderConfig config, final String location, final StreamGetter streamGetter)
	{

		final File file = getMediaCacheFile(config, location, streamGetter);

		if (file == null)
		{
			throw new IllegalStateException("Cannot get cached file");
		}

		return file;
	}

	//-----------------------------------------
	private File getMediaCacheFile(final MediaFolderConfig config, final String location, final StreamGetter streamGetter)
	{
		final File file;
		if (isStreamBiggerThanCacheSize(config, location, streamGetter))
		{
			file = getStreamAsTempFile(config, location, streamGetter);
		}
		else
		{
			file = new CacheResourceLoader<File>()
			{

				@Override
				public File getResource(final MediaCacheUnit cacheUnit)
				{
					return cacheUnit.getFile();
				}
			}.loadResource(config, location, streamGetter);
		}

		return file;
	}


	private static File getStreamAsTempFile(final MediaFolderConfig config, final String location, final StreamGetter streamGetter)
	{
		File file = null;
		final InputStream stream = streamGetter.getStream(config, location);
		try
		{
			file = File.createTempFile(location, "tmp");
			try (OutputStream out = Files.newOutputStream(file.toPath(), StandardOpenOption.DELETE_ON_CLOSE))
			{
				IOUtils.copy(stream, out);
			}

		}
		catch (final IOException e)
		{
			FileUtils.deleteQuietly(file);
			throw new IllegalStateException(
					"Cannot create temporary file for requested media from the storage [reason: " + e.getMessage() + "]", e);
		}
		return file;
	}

	//-----------------------------------------
	@Override
	public InputStream storeOrGetAsStream(final MediaFolderConfig config, final String location, final StreamGetter streamGetter)
	{
		final InputStream stream = getMediaCacheStream(config, location, streamGetter);

		if (stream == null)
		{
			throw new IllegalStateException("Cannot get cached file stream");
		}

		return stream;
	}

	//-----------------------------------------
	private InputStream getMediaCacheStream(final MediaFolderConfig config, final String location, final StreamGetter streamGetter)
	{
		final InputStream stream;

		if (isStreamBiggerThanCacheSize(config, location, streamGetter))
		{
			stream = streamGetter.getStream(config, location);
		}
		else
		{
			stream = new CacheResourceLoader<InputStream>()
			{

				@Override
				public InputStream getResource(final MediaCacheUnit cacheUnit)
				{
					return cacheUnit.getStream();
				}
			}.loadResource(config, location, streamGetter);
		}

		return stream;
	}

	private boolean isStreamBiggerThanCacheSize(final MediaFolderConfig config, final String location,
			final StreamGetter streamGetter)
	{
		final long sizeInBytes = streamGetter.getSize(config, location);
		final int cacheUnitWeight = MediaCacheUnit.convertNumBytesToCacheUnitWeight(sizeInBytes);

		return cacheUnitWeight > mediaCacheRegion.getCacheMaxEntries();
	}

	//-----------------------------------------
	private abstract class CacheResourceLoader<T>
	{
		public T loadResource(final MediaFolderConfig config, final String location, final StreamGetter streamGetter)
		{
			T resource = null;
			MediaCacheUnit cacheUnit;
			final MediaCacheKey key = new MediaCacheKey(tenantId, getCacheFolder(config), location);
			final RemoteStorageMediaLoader loader = new RemoteStorageMediaLoader(config, location, streamGetter);

			int retries = 0;
			while (resource == null && retries < GET_RESOURCE_MAX_RETRIES)
			{
				cacheUnit = cacheController.getWithLoader(key, loader);
				resource = getResource(cacheUnit);
				retries++;
			}

			return resource;
		}

		public abstract T getResource(final MediaCacheUnit cacheUnit);

	}

	private static String getCacheFolderPath(final MediaFolderConfig config)
	{
		final String rootCacheFolder = getCacheFolder(config);
		return rootCacheFolder + MediaUtil.FILE_SEP + config.getFolderQualifier();
	}

	private static String getCacheFolder(final MediaFolderConfig config)
	{
		return config.getParameter(DefaultSettingKeys.LOCAL_CACHE_ROOT_FOLDER_KEY.getKey(), String.class, DEFAULT_CACHE_FOLDER);
	}

	//-----------------------------------------
	/**
	 * Loads media binary data from remote storage and writes to disk cache.
	 */
	private class RemoteStorageMediaLoader implements CacheValueLoader<MediaCacheUnit>
	{
		private final MediaFolderConfig config;
		private final String originalLocation;
		private final StreamGetter streamGetter;
		private boolean loaded;
		private MediaCacheUnit cacheUnit;

		public RemoteStorageMediaLoader(final MediaFolderConfig config, final String originalLocation,
				final StreamGetter streamGetter)
		{
			this.config = config;
			this.originalLocation = originalLocation;
			this.streamGetter = streamGetter;
		}

		@Override
		public MediaCacheUnit load(@SuppressWarnings("unused")
		final CacheKey key)
		{
			cacheUnit = new MediaCacheUnit(storeMedia());
			loaded = true;
			return cacheUnit;
		}

		private File storeMedia()
		{
			final Map<String, Object> metaData = new HashMap<String, Object>();
			metaData.put(MediaMetaData.FOLDER_PATH, getCacheFolderPath(config));

			final StoredMediaData storedMediaData = storageStrategy.store(config, buildMediaId(originalLocation), metaData,
					streamGetter.getStream(config, originalLocation));
			return MediaUtil.composeOrGetParent(mainDataDir, storedMediaData.getLocation());
		}

		private String buildMediaId(final String location)
		{
			final String encodedLocation = Base64.getUrlEncoder().encodeToString(location.getBytes());
			final StringBuilder builder = new StringBuilder(encodedLocation);
			builder.append(CACHE_FILE_NAME_DELIM).append(UUID.randomUUID());
			return String.valueOf(builder.toString().hashCode());
		}

		public boolean isLoaded(final MediaCacheUnit cacheUnit)
		{
			// we need to compare references not values
			return loaded && cacheUnit == this.cacheUnit;
		}
	}

}