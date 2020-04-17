package org.training.platform.media.storage.impl;


import de.hybris.platform.core.Registry;
import de.hybris.platform.media.storage.LocalStoringStrategy;
import de.hybris.platform.media.storage.MediaMetaData;
import de.hybris.platform.media.storage.MediaStorageConfigService;
import de.hybris.platform.media.storage.MediaStorageConfigService.MediaFolderConfig;
import de.hybris.platform.media.storage.MediaStorageRegistry;
import de.hybris.platform.media.storage.MediaStorageStrategy;
import de.hybris.platform.media.storage.impl.DefaultLocalMediaFileCacheService;
import de.hybris.platform.media.storage.impl.DefaultMediaStorageConfigService.DefaultSettingKeys;
import de.hybris.platform.media.storage.impl.LocalFileMediaStorageStrategy;
import de.hybris.platform.media.storage.impl.MediaCacheRecreator;
import de.hybris.platform.media.storage.impl.MediaCacheRegion;
import de.hybris.platform.media.storage.impl.StoredMediaData;
import de.hybris.platform.regioncache.CacheController;
import de.hybris.platform.regioncache.CacheLifecycleCallback;
import de.hybris.platform.regioncache.CacheValueLoader;
import de.hybris.platform.regioncache.key.CacheKey;
import de.hybris.platform.regioncache.key.CacheUnitValueType;
import de.hybris.platform.regioncache.region.CacheRegion;
import de.hybris.platform.util.MediaUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Required;

import com.google.common.collect.Iterables;


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

	/**
	 * Recreates cache and adds lifecycle callback after bean construction
	 */
	@Override
	@PostConstruct
	public void init()
	{
		tenantId = Registry.getCurrentTenantNoFallback().getTenantID();
		cacheController.addLifecycleCallback(new MediaCacheLifecycleCallback());
		cacheRecreator.recreateCache(storageConfigService.getDefaultCacheFolderName(), getRemoteStorageFolderConfigs());
	}

	private Iterable<MediaFolderConfig> getRemoteStorageFolderConfigs()
	{
		final Set<MediaFolderConfig> result = new HashSet<>();

		final Map<String, MediaStorageStrategy> allStrategies = storageRegistry.getStorageStrategies();
		for (final Map.Entry<String, MediaStorageStrategy> entry : allStrategies.entrySet())
		{
			final String strategyId = entry.getKey();
			final MediaStorageStrategy strategy = entry.getValue();
			if (!(strategy instanceof LocalStoringStrategy))
			{
				Iterables.addAll(result, storageConfigService.getFolderConfigsForStrategy(strategyId));
			}
		}

		return result;
	}

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
			return builder.toString();
		}

		public boolean isLoaded(final MediaCacheUnit cacheUnit)
		{
			// we need to compare references not values
			return loaded && cacheUnit == this.cacheUnit;
		}
	}

	@Override
	public void removeFromCache(@SuppressWarnings("unused")
	final MediaFolderConfig config, final String location)
	{
		cacheController.invalidate(new MediaCacheKey(Registry.getCurrentTenant().getTenantID(), getCacheFolder(config), location));
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


	public static class MediaCacheKey implements CacheKey
	{
		private static final String MEDIA_CACHE_UNIT_CODE = "__MEDIA__";
		private final String tenantId;
		private final String location;
		private final String cacheFolder;

		public MediaCacheKey(final String tenantId, final String cacheFolder, final String location)
		{
			this.tenantId = tenantId;
			this.cacheFolder = cacheFolder;
			this.location = location;
		}

		@Override
		public CacheUnitValueType getCacheValueType()
		{
			return CacheUnitValueType.NON_SERIALIZABLE;
		}

		@Override
		public Object getTypeCode()
		{
			return MEDIA_CACHE_UNIT_CODE + cacheFolder;
		}

		@Override
		public String getTenantId()
		{
			return tenantId;
		}

		@Override
		public int hashCode()
		{
			final int prime = 31;
			int result = 1;
			result = prime * result + ((cacheFolder == null) ? 0 : cacheFolder.hashCode());
			result = prime * result + ((location == null) ? 0 : location.hashCode());
			result = prime * result + ((tenantId == null) ? 0 : tenantId.hashCode());
			return result;
		}

		@Override
		public boolean equals(final Object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj == null)
			{
				return false;
			}
			if (getClass() != obj.getClass())
			{
				return false;
			}
			final MediaCacheKey other = (MediaCacheKey) obj;
			if (cacheFolder == null)
			{
				if (other.cacheFolder != null)
				{
					return false;
				}
			}
			else if (!cacheFolder.equals(other.cacheFolder))
			{
				return false;
			}
			if (location == null)
			{
				if (other.location != null)
				{
					return false;
				}
			}
			else if (!location.equals(other.location))
			{
				return false;
			}
			if (tenantId == null)
			{
				if (other.tenantId != null)
				{
					return false;
				}
			}
			else if (!tenantId.equals(other.tenantId))
			{
				return false;
			}
			return true;
		}

		@Override
		public String toString()
		{
			return "MediaCacheKey [tenantId=" + tenantId + ", location=" + location + ", cacheFolder=" + cacheFolder + "]";
		}
	}



	private static class MediaCacheLifecycleCallback implements CacheLifecycleCallback
	{

		@Override
		public void onAfterRemove(final CacheKey key, final Object value, final CacheRegion region)
		{
			markAsEvictedAndTryRemove(value);
		}

		@Override
		public void onAfterEviction(final CacheKey key, final Object value, final CacheRegion region)
		{
			markAsEvictedAndTryRemove(value);
		}

		private static void markAsEvictedAndTryRemove(final Object cacheUnit)
		{
			if (cacheUnit instanceof MediaCacheUnit)
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Trying to remove cached file on eviction event [cacheUnit: " + cacheUnit + "]");
				}

				((MediaCacheUnit) cacheUnit).markResourceAsEvicted();
				((MediaCacheUnit) cacheUnit).tryRemoveResourceOrWriteEvictedMarker();
			}
		}

		@Override
		public void onAfterAdd(final CacheKey key, final Object value, final CacheRegion region)
		{
			// no need to implement
		}

		@Override
		public void onMissLoad(final CacheKey key, final Object value, final CacheRegion lruCacheRegion)
		{
			if (value instanceof MediaCacheUnit)
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Trying to remove file for unit " + value);
				}

				if (((MediaCacheUnit) value).isCachedFileExists())
				{
					final boolean isDeleted = ((MediaCacheUnit) value).getFile().delete();
					if (isDeleted)
					{
						if (LOG.isDebugEnabled())
						{
							LOG.debug("Removed cached file: " + ((MediaCacheUnit) value).getFile());
						}
					}
					else
					{
						LOG.error("Cannot remove cached file");
					}
				}

			}
		}
	}

	@Override
	@Required
	public void setMainDataDir(final File mainDataDir)
	{
		this.mainDataDir = mainDataDir;
	}

	@Override
	@Required
	public void setCacheController(final CacheController cacheController)
	{
		this.cacheController = cacheController;
	}

	@Override
	@Required
	public void setStorageRegistry(final MediaStorageRegistry storageRegistry)
	{
		this.storageRegistry = storageRegistry;
	}

	@Override
	@Required
	public void setStorageStrategy(final LocalFileMediaStorageStrategy storageStrategy)
	{
		this.storageStrategy = storageStrategy;
	}

	@Override
	@Required
	public void setMediaCacheRegion(final MediaCacheRegion mediaCacheRegion)
	{
		this.mediaCacheRegion = mediaCacheRegion;
	}

	@Override
	@Required
	public void setStorageConfigService(final MediaStorageConfigService storageConfigService)
	{
		this.storageConfigService = storageConfigService;
	}

	@Override
	@Required
	public void setCacheRecreator(final MediaCacheRecreator cacheRecreator)
	{
		this.cacheRecreator = cacheRecreator;
	}
}