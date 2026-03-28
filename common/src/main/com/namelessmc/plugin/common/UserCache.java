package com.namelessmc.plugin.common;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.namelessmc.java_api.NamelessAPI;
import com.namelessmc.java_api.exception.NamelessException;
import com.namelessmc.plugin.common.command.AbstractScheduledTask;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class UserCache implements Reloadable {

	private final NamelessPlugin plugin;

	private @Nullable AbstractScheduledTask task;
	private List<String> usernames = Collections.emptyList();
	private List<String> minecraftUsernames = Collections.emptyList();
	private List<UUID> minecraftUuids = Collections.emptyList();

	private static final String MINECRAFT_INTEGRATION = "Minecraft";

	UserCache(final NamelessPlugin plugin) {
		this.plugin = plugin;
	}

	@Override
	public void unload() {
		if (this.task != null) {
			this.task.cancel();
			this.task = null;
		}
		this.usernames = Collections.emptyList();
		this.minecraftUsernames = Collections.emptyList();
		this.minecraftUuids = Collections.emptyList();
	}

	@Override
	public void load() {
		this.refreshNow();
		task = this.plugin.scheduler().runTimer(this::refreshNow, Duration.ofMinutes(5));
	}

	public CompletableFuture<Void> refreshNow() {
		final CompletableFuture<Void> future = new CompletableFuture<>();
		this.plugin.scheduler().runAsync(() -> {
			this.plugin.logger().fine("Refreshing user cache");
			final NamelessAPI api = this.plugin.apiProvider().api();
			if (api == null) {
				future.complete(null);
				return;
			}

			try {
				final JsonArray users = api.requests()
						.get("users", "groups", null, "limit", 0)
						.getAsJsonArray("users");

				final List<String> usernames = new ArrayList<>(users.size());
				final List<String> minecraftUsernames = new ArrayList<>(users.size());
				final List<UUID> minecraftUuids = new ArrayList<>(users.size());

				for (final JsonElement element : users) {
					if (!element.isJsonObject()) {
						continue;
					}

					final JsonObject user = element.getAsJsonObject();
					final String username = this.getString(user, "username");
					if (username != null) {
						usernames.add(username);
					}

					final JsonObject minecraftIntegration = this.findVerifiedMinecraftIntegration(user);
					if (minecraftIntegration == null) {
						continue;
					}

					final String minecraftUsername = this.getString(minecraftIntegration, "username");
					if (minecraftUsername != null) {
						minecraftUsernames.add(minecraftUsername);
					}

					final String minecraftIdentifier = this.getString(minecraftIntegration, "identifier");
					if (minecraftIdentifier == null) {
						continue;
					}

					try {
						minecraftUuids.add(NamelessAPI.websiteUuidToJavaUuid(minecraftIdentifier));
					} catch (final IllegalArgumentException e) {
						this.plugin.logger().warning("Skipping invalid Minecraft UUID from user cache refresh: " + minecraftIdentifier);
					}
				}

				this.usernames = Collections.unmodifiableList(usernames);
				this.minecraftUsernames = Collections.unmodifiableList(minecraftUsernames);
				this.minecraftUuids = Collections.unmodifiableList(minecraftUuids);
				future.complete(null);
			} catch (NamelessException e) {
				this.plugin.logger().logException(e);
				future.completeExceptionally(e);
			}
		});
		return future;
	}

	private List<String> search(Collection<String> original, @Nullable String part) {
		if (part == null) {
			return Collections.emptyList();
		}
		return original.stream().filter(s -> s.startsWith(part)).collect(Collectors.toUnmodifiableList());
	}

	public List<String> usernames() {
		return this.usernames;
	}

	public List<String> usernamesSearch(@Nullable String part) {
		return this.search(this.usernames, part);
	}

	public List<String> minecraftUsernames() {
		return this.minecraftUsernames;
	}

	public List<UUID> minecraftUuids() {
		return this.minecraftUuids;
	}

	public List<String> minecraftUsernamesSearch(@Nullable String part) {
		return this.search(this.minecraftUsernames, part);
	}

	private @Nullable JsonObject findVerifiedMinecraftIntegration(final JsonObject user) {
		if (!user.has("integrations") || !user.get("integrations").isJsonArray()) {
			return null;
		}

		for (final JsonElement element : user.getAsJsonArray("integrations")) {
			if (!element.isJsonObject()) {
				continue;
			}

			final JsonObject integration = element.getAsJsonObject();
			if (!MINECRAFT_INTEGRATION.equals(this.getString(integration, "integration"))) {
				continue;
			}

			if (!integration.has("verified") || !integration.get("verified").getAsBoolean()) {
				return null;
			}

			return integration;
		}

		return null;
	}

	private @Nullable String getString(final JsonObject object, final String property) {
		if (!object.has(property) || object.get(property).isJsonNull()) {
			return null;
		}

		return object.get(property).getAsString();
	}

}
